import sys
from record import Record 
'''
modified Treenode and AVL_Tree

Author: geeksforgeeks
url: https://www.geeksforgeeks.org/avl-tree-set-1-insertion/
'''

# Treenode class for AVL Tree
class TreeNode(object): 
    def __init__(self, val = None, data = None): 
        self.val = val 
        self.data = data
        self.left = None
        self.right = None
        self.height = 1


class AVL_Tree(object): 
  
    # Recursive function to insert a key-value pair in  
    # subtree rooted with node and returns 
    # new root of subtree. 

    def insert(self, root, key, value): 
      
        # Step 1 - Perform normal BST 
        if not root: 
            return TreeNode(key, value) 
        elif key < root.val: 
            root.left = self.insert(root.left, key, value) 
        else: 
            root.right = self.insert(root.right, key, value) 
  
        # Step 2 - Update the height of the  
        # ancestor node 
        root.height = 1 + max(self.getHeight(root.left), 
                           self.getHeight(root.right)) 
  
        # Step 3 - Get the balance factor 
        balance = self.getBalance(root) 
  
        # Step 4 - If the node is unbalanced,  
        # then try out the 4 cases 
        # Case 1 - Left Left 
        if balance > 1 and key < root.left.val: 
            return self.rightRotate(root) 
  
        # Case 2 - Right Right 
        if balance < -1 and key > root.right.val: 
            return self.leftRotate(root) 
  
        # Case 3 - Left Right 
        if balance > 1 and key > root.left.val: 
            root.left = self.leftRotate(root.left) 
            return self.rightRotate(root) 
  
        # Case 4 - Right Left 
        if balance < -1 and key < root.right.val: 
            root.right = self.rightRotate(root.right) 
            return self.leftRotate(root) 
  
        return root 
  
    def leftRotate(self, z): 
  
        y = z.right 
        T2 = y.left 
  
        # Perform rotation 
        y.left = z 
        z.right = T2 
  
        # Update heights 
        z.height = 1 + max(self.getHeight(z.left), 
                         self.getHeight(z.right)) 
        y.height = 1 + max(self.getHeight(y.left), 
                         self.getHeight(y.right)) 
  
        # Return the new root 
        return y 
  
    def rightRotate(self, z): 
  
        y = z.left 
        T3 = y.right 
  
        # Perform rotation 
        y.right = z 
        z.left = T3 
  
        # Update heights 
        z.height = 1 + max(self.getHeight(z.left), 
                        self.getHeight(z.right)) 
        y.height = 1 + max(self.getHeight(y.left), 
                        self.getHeight(y.right)) 
  
        # Return the new root 
        return y 
  
    def getHeight(self, root): 
        if not root: 
            return 0
  
        return root.height 
  
    def getBalance(self, root): 
        if not root: 
            return 0
  
        return self.getHeight(root.left) - self.getHeight(root.right) 
  
    def getInOrder(self, root, searchArea = False, area = ''):
        '''
        return the in-order string
        used for printing and getting min an max key value,

        searchArea: True, returns the list of records matched by area code
        '''
        if not root: 
            return []

        res = []
        l = self.getInOrder(root.left, searchArea, area) 
        r = self.getInOrder(root.right, searchArea, area)

        if not searchArea:
            if l: res += l
            res.append(root.data)
            if r: res += r

        else:
            #retrive records by area code
            if l: res += l
            if root.data.phone.split('-')[0] == area:
                res.append(root.data)
            if r: res += r
            
        
        
        return res

    def searchNode(self, root: TreeNode, key):
        '''
        return the node with the key
        '''
        curr = root
        while curr:
            if curr.val == key:
                return curr
            if curr.val > key:
                curr = curr.left
            else:
                curr = curr.right 
            
        return None


# class SSKey:
#     def __init__(self, key: tuple):
#         self.key = key
    
#     def __lt__(self, other):
#         return self.key[0] < other[0]
    
#     def __getitem__(self, ind):
#         return self.key[ind]
        
#     def __str__(self):
#         return str(self.key)


class SSTable:
    
    def __init__(self, num_of_rec_allowed = 5, IMMUTABLE = False, data:bytearray = None):
        #use an AVL tree to keep key strings sorted
        self.records = AVL_Tree()
        self.root = None
        self.num_of_rec = 0
        self.num_of_rec_allowed = num_of_rec_allowed

        #key range used for merging
        self.lo = None
        self.hi = None

        #whether it has stored on disk
        self.IMMUTABLE = IMMUTABLE
        if self.IMMUTABLE:
            self.records = bytearray

    def __str__(self):
        return str(self.records.getInOrder(self.root))

    def add(self, rec: Record):
        '''
        add a write record
        '''       
        key = rec.id
        node = self.records.searchNode(self.root, key)
        if node:
            node.data = rec
            # print('overwrite',node)
            return 1
        else:
            self.root = self.records.insert(self.root, key, rec)
            self.num_of_rec += 1
            return 0
        return -1
   
    def get_num_records(self):
        return self.num_of_rec

    def getInOrder(self):
        return self.records.getInOrder(self.root)



    def search_rec(self, key):
        '''
        fetches the TreeNode that pk maps to 
        returns the record within that node
        '''
        if not self.IMMUTABLE:
            node = self.records.searchNode(self.root, key)
        #TODO: binary search bytearray for key if IMMUTABLE
        
        if node: 
            return node.data
        return node

    def search_recs(self, area):
        '''
        fetches the TreeNodes are the area code maps to 
        returns the list of records with those nodes
        '''
        if not self.IMMUTABLE:
            nodes = self.records.getInOrder(self.root, True, area)
        #TODO: linear search bytearray for recs if IMMUTABLE 
        
        return nodes


    
    def getKeyRange(self):
        '''
        used for SSTable merging
        O(n) runtime
        '''
        sortedKeys = self.records.getInOrder(self.root)
        return (sortedKeys[0], sortedKeys[-1])

    def isFull(self):
        '''
        return True if SSTable is full
        '''
        return self.num_of_rec == self.num_of_rec_allowed


if __name__ == '__main__':
    
    if len(sys.argv) == 2 and sys.argv[1] == 'test':
        sstable = SSTable()

        sstable.add(Record(123, 'test', '401-222-3142'))
        sstable.add(Record(122, 'test', '401-222-0000'))
        sstable.add(Record(10, 'test', '412-222-3142'))
        print(sstable.num_of_rec)

        sstable.add(Record(13, 'test', '412-222-3142'))
        print(sstable.num_of_rec)

        sstable.add(Record(122, 'test', '333-222-9999'))
        print(sstable.num_of_rec)

        sstable.add(Record(100, 'test', '333-222-3143'))
        
        print(sstable.records.getInOrder(sstable.root))

        print(sstable.search_rec(100))
        print(sstable.search_rec(122))

        print(sstable.search_recs('333'))
        print(sstable.search_recs('412'))
        
        for x in sstable.getKeyRange(): print(str(x))
        print(sstable)
        print(sstable.isFull())
    

  

