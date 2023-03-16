import pkg_resources


def list_updater_names(entry_name="asreview.preprocess.updaters"):
    return [*get_entry_points(entry_name)]


def list_localdb_names(entry_name="asreview.preprocess.localdbs"):
    return [*get_entry_points(entry_name)]


def list_deduplicator_names(entry_name="asreview.preprocess.deduplicators"):
    return [*get_entry_points(entry_name)]


def get_entry_points(entry_name="asreview.preprocess.entry_points"):
    """Get the entry points for asreview preprocess.
    Parameters
    ----------
    entry_name: str
        Name of the submodule. Default "asreview.preprocess.entry_points".
    Returns
    -------
    dict:
        Dictionary with the name of the entry point as key
        and the entry point as value.
    """
    return {entry.name: entry for entry in pkg_resources.iter_entry_points(entry_name)}


def _updater_class_from_entry_point(method, entry_name="asreview.preprocess.updaters"):
    entry_points = get_entry_points(entry_name)
    try:
        return entry_points[method].load()
    except KeyError:
        raise ValueError(
            f"Error: method '{method}' is not implemented for entry point "
            f"{entry_name}."
        )
    except ImportError as e:
        raise ValueError(
            f"Failed to import '{method}' updater ({entry_name}) "
            f"with the following error:\n{e}"
        )


def _localdb_class_from_entry_point(suffix, entry_name="asreview.preprocess.localdbs"):
    entry_points = get_entry_points(entry_name)
    try:
        return entry_points[suffix].load()
    except KeyError:
        raise ValueError(
            f"Error: suffix '{suffix}' is not implemented for entry point "
            f"{entry_name}."
        )
    except ImportError as e:
        raise ValueError(
            f"Failed to import '{suffix}' localdb ({entry_name}) "
            f"with the following error:\n{e}"
        )


def _deduplicator_class_from_entry_point(
    suffix, entry_name="asreview.preprocess.deduplicators"
):
    entry_points = get_entry_points(entry_name)
    try:
        return entry_points[suffix].load()
    except KeyError:
        raise ValueError(
            f"Error: suffix '{suffix}' is not implemented for entry point "
            f"{entry_name}."
        )
    except ImportError as e:
        raise ValueError(
            f"Failed to import '{suffix}' deduplicator ({entry_name}) "
            f"with the following error:\n{e}"
        )
