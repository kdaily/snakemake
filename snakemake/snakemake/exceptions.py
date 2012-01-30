import sys, traceback
from collections import defaultdict

def print_exception(ex, rowmap):
	for file, lineno, _, _ in traceback.extract_tb(ex.__traceback__):
			if file == "<string>":
				print("Error in line {} of Snakefile:\n{}".format(rowmap[lineno], str(ex)), file = sys.stderr)
				return
	if not isinstance(ex, RuleException):
		traceback.print_tb(ex.__traceback__)
	print(ex, file=sys.stderr)

class RuleException(Exception):
	pass

class MissingOutputException(RuleException):
	pass

class MissingInputException(RuleException):
	def __init__(self, rule = None, files = None, include = None):
		self.missing = defaultdict(set)
		if files and rule:
			self.missing[rule].update(files)
		if include:
			for ex in include:
				for rule, files in ex.missing.items():
					self.missing[rule].update(files)
	
	def __str__(self):
		s = ""
		for rule, files in self.missing.items():
			s += "Missing input files for rule {}:\n{}\n".format(rule, ", ".join(files))
		return s

class AmbiguousRuleException(RuleException):
	def __init__(self, rule1, rule2):
		super(AmbiguousRuleException, self).__init__("Ambiguous rules: {} and {}.".format(rule1, rule2))

class CyclicGraphException(RuleException):
	def __init__(self, rule):
		super(CyclicGraphException, self).__init__("Cyclic dependency on rule {}.".format(rule))
		
class MissingRuleException(RuleException):
	def __init__(self, file):
		super(MissingRuleException, self).__init__("No rule to produce {}.".format(file))

class CreateRuleException(RuleException):
	pass