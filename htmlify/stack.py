class UniqueStack():
    def __init__(self, items):
        self._items = set(items or [])
        self._seen = set(items or [])

    @property
    def items(self):
        return list(self._seen)    

    def __iter__(self):
        return self

    def __next__(self):
        if self._items:
            return self._items.pop()
        raise StopIteration
    
    def __len__(self):
        return len(self.items)    

    def __iadd__(self, item):
        if not item in self._seen:
            self._items.add(item)
            self._seen.add(item)
        return self

    def update(self, new_items):
        unseen = set(new_items).difference(self._seen)
        self._items.update(unseen)
        self._seen.update(unseen)  

    def discard(self, item):
        self._items.discard(item)
        self._seen.discard(item)          