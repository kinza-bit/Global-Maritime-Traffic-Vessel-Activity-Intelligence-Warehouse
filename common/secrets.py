import os


def get_secret(name, scope="maritime-kv"):
    """Read a secret from the Databricks secret scope.
    Local fallback: an environment variable named like the secret
    (gfw-api-token -> GFW_API_TOKEN). The value is never printed."""
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils.secrets.get(scope, name)
    except Exception:
        value = os.environ.get(name.upper().replace("-", "_"))
        if value is None:
            raise RuntimeError(f"Secret '{name}' not found")
        return value
