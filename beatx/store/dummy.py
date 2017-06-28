class Store:
    def __init__(self, store_url=None):
        pass

    def load_entries(self):
        return {}

    def save_entries(self, entries):
        pass

    def has_locked(self):
        return True

    def acquire_lock(self, lock_ttl=None):
        return True

    def renew_lock(self):
        pass

    def release_lock(self):
        pass
