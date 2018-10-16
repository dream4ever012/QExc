# -*- coding: utf-8 -*-
"""
Created on Thu Sep 13 22:22:49 2018

@author: hkim85
"""
from collections import defaultdict
import copy

class Query(object):
    """ Graph data structure, undirected by default. """

    def __init__(self, connections, directed=False, isVK = False):
        self._graph = defaultdict(set) # must update _graph_vk whenever update _graph
        self._directed = directed
        self.add_connections(connections)
        if (isVK == False): self.query_vk = self.swapKeyVal()
        """ mJUhstry keeps a path for query execution: mJU carries proxy stats for tbls """
        """ having a swapped grpah may not be a good idea as this may not be the up-to-date"""

    def add_connections(self, connections):
        """ Add connections (list of tuple pairs) to graph """
        
        for node1, node2 in connections:
            self.add(node1, node2)
    
    def add(self, node1, node2):
        """ Add connection between node1 and node2 """

        self._graph[node1].add(node2)
        if not self._directed:
            self._graph[node2].add(node1)
        
    def remove(self, node):
        """ Remove all references to node """
        for n, cxns in self._graph.iteritems():
            try:
                cxns.remove(node)
            except KeyError:
                pass
        try:
            del self._graph[node]
        except KeyError:
            pass
    """
    # cherry pick remove 
    def remove_CP(self, node_key, node_ele):
    """
    
    def getKeys(self):
        return self._graph.keys()
    
    def getRelTbl(self, TM):
        return self._graph_vk[TM] 
    
    def getFstKey(self):
        return self._graph.items()[0][0]
        
    def getQueryGraph(self):
        return self._graph
    
    def getQuery_vk(self):
        return self.query_vk
        
    def concatKeyNames(self):
        temp = []
        for key in self._graph:
            temp.append(key.getTableName())
        return '_'.join(temp)
    
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            """ Keys are not empty and values are not empty """
            is_equal = True
            # key comparision
            self_keys = self.getKeys()
            self_keys.sort(key=lambda node: node.table_name)
            theO_keys = other.getKeys()
            theO_keys.sort(key=lambda node: node.table_name)
            if self_keys != theO_keys: is_equal = False
            
            # value comparison
            for self_key, theO_key in zip(self_keys, theO_keys):
                self_values = list(self._graph[self_key])
                self_values.sort(key=lambda node: node.table_name)
                other_values = list(other._graph[theO_key])
                other_values.sort(key=lambda node: node.table_name)
                if (self_values != other_values): is_equal = False
            return is_equal
        else: return False
            

    def getValues(self, key):
        return self._graph[key]

    def get_AllNodes_set(self):
        allNodes = set([])    
        #allNodes = self.getKeys()
        for key in self._graph:
            allNodes.add(key)
            for node in self._graph[key]:
                allNodes.add(node)
        return allNodes
        
    def get_AllNodes(self):
        allNodes = set([])    
        #allNodes = self.getKeys()
        for key in self._graph:
            allNodes.add(key)
            for node in self._graph[key]:
                allNodes.add(node)
        return sorted(list(allNodes), key=lambda node: node.table_name)
        
    def getAllValues(self):
        """ input  : query object """
        """ output : TM Set """
        temp = set([])
        for key in self._graph:
            temp = temp.union(self.getValues(key))
        return temp
    
    def swapKeyVal(self):
        """ input query object """
        """ swap key-value pair """
        conn = [(val,key) for key in self._graph for val in self._graph[key]]
        return Query(conn, directed=True, isVK = True)
    




