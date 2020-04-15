import unittest

from common import *
from inst import Instruction
from instruction_sequence import InstructionSequence
from parser import Parser
from record import Record

filename = "tests/phase2_test_script.txt"

inst_seq = [
    Instruction(ACTION.RETRIEVE_BY_ID, 'X', 13),
    Instruction(ACTION.WRITE_RECORD, 'X', Record(2, "Thalia", "412-656-2212")),
    Instruction(ACTION.RETRIEVE_BY_ID, 'X', 2),
    Instruction(ACTION.RETRIEVE_BY_AREA_CODE, 'X', "412"),
    Instruction(ACTION.COMMIT)
]

seq1 = InstructionSequence(inst_seq, EXEC_TYPE.TRANSACTION)
seq2 = InstructionSequence(inst_seq, EXEC_TYPE.PROCESS)

class TestParser(unittest.TestCase):
    def test_phase2(self):
        seqs = Parser.parse(filename, create_transaction=True)

        self.assertEqual(seq1.exec_type, seqs[0].exec_type)
        self.assertEqual(seq2.exec_type, seqs[1].exec_type)

        for i in range(0, len(inst_seq)):
            self.assertEqual(seq1.instruction_sequence[i], seqs[0].instruction_sequence[i])
            self.assertEqual(seq2.instruction_sequence[i], seqs[1].instruction_sequence[i])

if __name__ == '__main__':
    unittest.main()
