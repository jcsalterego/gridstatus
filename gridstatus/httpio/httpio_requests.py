import requests

from gridstatus.httpio.auto_adapter_dispatcher import AutoAdapterDispatcher


class HttpioRequests(AutoAdapterDispatcher):
    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(HttpioRequests, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        super().__init__()

    def get(self, *args, **kwargs):
        value = self._before_hook("get", args, kwargs)
        return value or requests.get(*args, **kwargs)

    def post(self, *args, **kwargs):
        value = self._before_hook("post", args, kwargs)
        return value or requests.post(*args, **kwargs)

    class Session(AutoAdapterDispatcher):
        def __init__(self):
            super().__init__()
            self.session = requests.Session()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.session.close()

        def get(self, *args, **kwargs):
            value = self._before_hook("session.get", args, kwargs)
            return value or self.session.get(*args, **kwargs)

        def post(self, *args, **kwargs):
            value = self._before_hook("session.post", args, kwargs)
            return value or self.session.post(*args, **kwargs)

        def __getattr__(self, item):
            return getattr(self.session, item)