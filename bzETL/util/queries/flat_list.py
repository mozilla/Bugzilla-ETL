

class FlatList(list):
    """
    FlatList IS A RESULT OF FILTERING SETS OF TREES
    WE SAVED OURSELVES FROM COPYING ALL OBJECTS IN ALL PATHS OF ALL TREES,
    BUT WE ARE LEFT WITH THIS LIST OF TUPLES THAT POINT TO THE SAME
    """

    def __init__(self, path, data):
        """
        data IS A LIST OF TUPLES
        EACH TUPLE IS THE SEQUENCE OF OBJECTS FOUND ALONG A PATH IN A TREE
        IT IS EXPECTED len(data[i]) == len(path)+1 (data[i][0] IS THE ORIGINAL ROW OBJECT)
        """
        list.__init__(self)
        self.data=data
        self.path=path


