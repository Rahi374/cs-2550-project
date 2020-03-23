'''
storage.py  
'''
from common import *
from enum import Enum
import os, shutil, stat, sys

class ORG(Enum):
    SEQ = 1
    LSM = 2


class Block:
    '''
    disk block used for storing SP/SSTable nodes
    call add method to add write records 
    '''
    def __init__(self, size = 32, data: bytearray = None):
        '''
        size: # of bytes 
        '''
        self.size = size
        self.data = data

    def add(self, data: bytearray):
        if len(self.data) + len(data) >= self.size:
            return -1
        else:
            self.data += data
            return 0

    def __getitem__(self, ind):
        return self.data[ind]

    def __str__(self):
        return str(self.data)

            

class Storage():
    file_org = None
    org_str = None
    blk_size = None
    mnt_path = None

    def __init__(self, org: ORG, blk_size: int, mnt: str = 'storage/'):
        #mount point 'storage/'
        self.file_org = org
        self.org_str = 'SEQ' if org == ORG.SEQ else 'LSM'
        self.blk_size = blk_size
        self.mnt_path = mnt 
        
        #2nd mount point
        add_mnt = 'storage_' + str(blk_size) + '_' + self.org_str
        self.create_tbl(add_mnt)
        self.mnt_path += add_mnt + '/'

    def create_tbl(self, tbl_name: str):
        path = self.mnt_path + tbl_name + '/'
        os.mkdir(path)
        self.rm_dir_readonly(path)

    def rm_tbl(self, tbl_name: str, cascade = True):
        if cascade:
            shutil.rmtree(self.mnt_path + tbl_name + '/')
        else:
            os.rmdir(self.mnt_path + tbl_name + '/')
        
    def read_blk(self, tbl_name: str, blk_id: int):
        f_path = self.get_blk_path(tbl_name, blk_id)

        if not os.path.exists(self.mnt_path + tbl_name):
            raise Exception('table does not exist')
        elif not os.path.exists(f_path):
            raise Exception('block does not exist')
        else:
            f = open(f_path, 'rb')
            blk = bytearray(f.read())
            f.close()
            return blk
            
    def write_blk(self, tbl_name: str, blk_id: int, blk: bytearray):
        
        if not len(blk) == self.blk_size:
            raise Exception('invalid block size')

        f_path = self.get_blk_path(tbl_name, blk_id)
        if not os.path.exists(self.mnt_path + tbl_name):
            raise Exception('table does not exist')
        else:
            f = open(f_path, 'wb')
            f.write(blk)
            f.close()
            return 0

    def rm_blk(self, tbl_name: str, blk_id: int):
        os.remove(self.get_blk_path(tbl_name, blk_id))
        return 0

    def rm_dir_readonly(self, path: str):
        '''
        remove os's readonly flag on newly created directory
        path: path to directory
        '''
        os.chmod(path, 0o777)

    def rm_file_readonly(self, path: str):
        '''
        remove os's readonly flag on newly created file
        path: path to file
        '''
        os.chmod(path, 0o666)

    def get_blk_path(self, tbl_name, blk_id):
        return self.mnt_path + tbl_name + '/' + 'block_' + str(blk_id) + '.img'

    

if __name__ == '__main__':

    def check(e, e_test):
        if not str(e) == str(e_test):
            print('\033[91m test case failed \033[0m')
            raise Exception('\033[91m\nexpected: ' + str(e_test) + '\nobserved: ' + str(e) + '\n\033[0m')
        else:
            print('\33[32m test case passed \033[0m')

    if len(sys.argv) == 2 and sys.argv[1] == 'test':

        #test constructor
        sto = Storage(ORG.LSM, 32)
        check(sto.org_str + ' ' + str(sto.blk_size) + ' ' + sto.mnt_path, 'LSM 32 storage/storage_32_LSM/')

        #test create tables
        for x in range(5): sto.create_tbl('x_' + str(x))
        sto.create_tbl('x_1/1_new')

        #test write blk + create blk, read blk (too lazy to test read and write separately)
        sto.write_blk('x_3', 23, 'datadatadatadatadatadatadatadata')
        sto.write_blk('x_4', 1908, 'whatwhatwhatwhatwhatwhatwhatwhat')
        check(sto.read_blk('x_3', 23), 'datadatadatadatadatadatadatadata')
        check(sto.read_blk('x_4', 1908), 'whatwhatwhatwhatwhatwhatwhatwhat')

        #test overwrite blk, read blk 
        sto.write_blk('x_3', 23, '||||||||||||||||||||||||||||||||')
        check(sto.read_blk('x_3', 23), '||||||||||||||||||||||||||||||||')

        #test write with invalid blk size
        try:
            sto.write_blk('x_4', 10, 'testtesttesttesttest')
        except Exception as e:
            check(e, 'invalid block size')

        #test write with table does not exist
        try:
            sto.write_blk('yy', 10, 'testtesttesttesttesttesttesttest')
        except Exception as e:
            check(e, 'table does not exist')

        #test read with table does not exist
        try:
            sto.read_blk('bb', 123)
        except Exception as e:
            check(e, 'table does not exist')

        #test read with block does not exist
        try:
            sto.read_blk('x_3', 123)
        except Exception as e:
            check(e, 'block does not exist')

        #test delete tables (not cascade)
        sto.rm_tbl('x_0', cascade=False)
        testset = set(os.listdir(sto.mnt_path)) 
        for x in range(1, 5): testset.remove('x_' + str(x))
        check(testset, 'set()')

        #test delete tables (cascade)
        sto.rm_tbl('x_4')
        testset = set(os.listdir(sto.mnt_path)) 
        for x in range(1, 4): testset.remove('x_' + str(x))
        check(testset, 'set()')

        #test delete block
        sto.rm_blk('x_3', 23)
        testset = set(os.listdir(sto.mnt_path + 'x_3' + '/')) 
        check(testset, 'set()')





