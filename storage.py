'''
storage.py  

'''
# from core import ORG
from enum import Enum
import os, shutil, stat, sys

class ORG(Enum):
	SEQ = 1
	LSM = 2

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
		#TODO: implement cascade, create test cases
		os.rmdir(self.mnt_path + tbl_name + '/')
		
	def rm_dir_readonly(self, path: str):
		os.chmod(path, 0o777)

	def rm_file_readonly(self, path: str):
		os.chmod(path, 0o666)

	def read_blk(self, tbl_name: str, blk_id: int):
		f_path = self.mnt_path + tbl_name + '/' + 'block_' + str(blk_id) + '.img'

		if not os.path.exists(self.mnt_path + tbl_name):
			raise Exception('table does not exist')
		elif not os.path.exists(f_path):
			raise Exception('block does not exist')
		else:
			f = open(f_path, 'r')
			blk = f.read()
			f.close()
			return blk
			
		
	def write_blk(self, tbl_name: str, blk_id: int, blk):

		if not len(blk) == self.blk_size:
			raise Exception('invalid block size')

		f_path = self.mnt_path + tbl_name + '/' + 'block_' + str(blk_id) + '.img'
		if not os.path.exists(self.mnt_path + tbl_name):
			raise Exception('table does not exist')
		else:
			f = open(f_path, 'w+')
			f.write(blk)
			f.close()
			return 0

	def rm_blk(self, tbl_name: str, blk_id: int):
		#TODO: implement
		return 0
		
		
	
		

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

		

		

		

		
		




