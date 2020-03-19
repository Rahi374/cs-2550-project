




import sys

class SSKey:
    def __init__(self, key: tuple):
        self.key = key
    
    def __lt__(self, other):
        if self.key[0] == other[0]:
            return self.key[1] < other[1]
        else:
            return self.key[0] < other[0]
    
    def __getitem__(self, ind):
        return self.key[ind]
        
    def __str__(self):
        return str(self.key)


'''
Author: GeeksforGeeks
URL: https://www.geeksforgeeks.org/avl-tree-set-1-insertion/
'''

# Python code to insert a node in AVL tree 
# Generic tree node class 

class TreeNode(object): 
    def __init__(self, val = None, data = None): 
        self.val = val 
        self.data = data
        self.left = None
        self.right = None
        self.height = 1

# AVL tree class which supports the  
# Insert operation 
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
  
    def preOrder(self, root): 
  
        if not root: 
            return
  
        print("{0} ".format(root.val), end="") 
        self.preOrder(root.left) 
        self.preOrder(root.right) 

    def inOrder(self, root):
        if not root:
            return 

        self.inOrder(root.left)
        print("{0} ".format(root.val), end="") 
        self.inOrder(root.right)

    


class SSTable:
    def __init__(self):
        #use an AVL tree to keep key strings sorted
        self.records = AVL_Tree()
        self.root = None
    
    def add(self, tbl_name, key, record):
        '''
        add a write record
        '''        
        sskey = SSKey((tbl_name, key))
        self.root = self.records.insert(self.root, sskey, record)



if __name__ == '__main__':
    
    if len(sys.argv) == 2 and sys.argv[1] == 'test':
        sstable = SSTable()
        sstable.add('abc', 12, 0000000)
        sstable.add('abc', 13, 0000000)
        sstable.add('aaa', 13, 0000000)
        sstable.add('adc', 11, 0000000)
        sstable.add('aaa', 1, 0000000)
        sstable.records.inOrder(sstable.root)
        print()

  

