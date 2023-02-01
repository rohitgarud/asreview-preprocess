from flask import Flask, render_template, request, redirect, url_for

try:
    from flask_sqlalchemy import SQLAlchemy
except ImportError:
    print(
        "The GUI requires the flask-SQLAlchemy package to be installed. Install optional ASReview-preprocess dependencies specific for GUI with 'pip install asreview-preprocess[gui]' or all dependencies with 'pip install asreview-preprocess[all]'"
    )

app = Flask(__name__)

##CREATE DATABASE
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///preprocess.db"
# Optional: But it will silence the deprecation warning in the console.
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


##CREATE TABLE
# class Record(db.Model):
#     id = db.Column(db.Integer, primary_key=True)
#     title = db.Column(db.String(500), unique=True, nullable=False)
#     author = db.Column(db.String(250), nullable=False)
#     rating = db.Column(db.Float, nullable=False)


# db.create_all()


@app.route("/")
def home():
    ##READ ALL RECORDS
    # all_records = db.session.query(Record).all()
    # return render_template("index.html", records=all_records)
    return "<h1>Welcome to ASReview Preprocess</h1>"


def launch_gui():
    app.run(debug=True)
