import logging
import os
from gevent.pywsgi import WSGIServer

from flask import Flask, render_template, request, redirect, url_for, flash
from flask_cors import CORS
from flask.json import jsonify
from werkzeug.exceptions import InternalServerError
from werkzeug.utils import secure_filename

from asreview.webapp.start_flask import _check_port_in_use, _open_browser

try:
    from flask_sqlalchemy import SQLAlchemy
    from flask_bootstrap import Bootstrap
except ImportError:
    print(
        "The GUI requires additional packages to be installed. Install optional ASReview-preprocess dependencies specific for GUI with 'pip install asreview-preprocess[gui]' or all dependencies with 'pip install asreview-preprocess[all]'"
    )

# set logging level
if os.environ.get("FLASK_ENV", "") == "development":
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)


def create_app(**kwargs):
    UPLOAD_FOLDER = "asreviewcontrib\preprocess\work_dir"
    ALLOWED_EXTENSIONS = {"csv"}

    app = Flask(
        __name__,
        instance_relative_config=True,
        static_folder="static",
        template_folder="templates",
    )

    Bootstrap(app)

    app.config["preprocess_kwargs"] = kwargs
    app.config["BOOTSTRAP_SERVE_LOCAL"] = True
    app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

    def allowed_file(filename):
        return (
            "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS
        )

    # Ensure the instance folder exists.
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    CORS(app, resources={r"*": {"origins": "*"}})

    @app.errorhandler(InternalServerError)
    def error_500(e):
        original = getattr(e, "original_exception", None)

        if original is None:
            # direct 500 error, such as abort(500)
            logging.error(e)
            return jsonify(message="Whoops, something went wrong."), 500

        # wrapped unhandled error
        logging.error(e.original_exception)
        return jsonify(message=str(e.original_exception)), 500

    @app.route("/", methods=["GET"])
    def index(**kwargs):
        return render_template("index.html")

    @app.route("/file/", methods=["GET", "POST"])
    def upload_file():
        if request.method == "POST":
            if "file" not in request.files:
                flash("No file part")
                return redirect(request.url)
            file = request.files["file"]

            if file.filename == "":
                flash("No selected file")
                return redirect(request.url)
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config["UPLOAD_FOLDER"], filename))
                return redirect(request.url)
        return """
        <!doctype html>
        <title>Upload new File</title>
        <h1>Upload new File</h1>
        <form method=post enctype=multipart/form-data>
        <input type=file name=file>
        <input type=submit value=Upload>
        </form>
        """

    return app


def launch_gui(args):
    app = create_app()
    app.config["PROPAGATE_EXCEPTIONS"] = False
    from werkzeug.utils import secure_filename

    # app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///preprocess.db"
    # app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    # db = SQLAlchemy(app)

    # ssl certificate, key and protocol
    certfile = args.certfile
    keyfile = args.keyfile
    ssl_context = None
    if certfile and keyfile:
        protocol = "https://"
        ssl_context = (certfile, keyfile)
    else:
        protocol = "http://"

    flask_dev = os.environ.get("FLASK_ENV", "") == "development"
    host = args.ip
    port = args.port
    port_retries = args.port_retries
    # if port is already taken find another one
    if not os.environ.get("FLASK_ENV", "") == "development":
        original_port = port
        while _check_port_in_use(host, port) is True:
            old_port = port
            port = int(port) + 1
            if port - original_port >= port_retries:
                raise ConnectionError(
                    "Could not find an available port \n"
                    "to launch ASReview LAB. Last port \n"
                    f"was {str(port)}"
                )
            print(f"Port {old_port} is in use.\n* Trying to start at {port}")

    # open webbrowser if not in flask development mode
    if flask_dev is False:
        _open_browser(host, port, protocol, args.no_browser)

    # run app in flask mode only if flask_env == development is True
    if flask_dev is True:
        app.run(host=host, port=port, ssl_context=ssl_context)
    else:
        ssl_args = {"keyfile": keyfile, "certfile": certfile} if ssl_context else {}
        server = WSGIServer((host, port), app, **ssl_args)
        server.serve_forever()
