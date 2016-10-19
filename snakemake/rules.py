__author__ = "Johannes Köster"
__copyright__ = "Copyright 2015, Johannes Köster"
__email__ = "koester@jimmy.harvard.edu"
__license__ = "MIT"

import os
import re
import sys
import inspect
import sre_constants
from collections import defaultdict, Iterable

#RASMUS
from snakemake.io import IOFile, _IOFile, protected, temp, dynamic, ancient, Namedlist, AnnotatedString, contains_wildcard_constraints, update_wildcard_constraints
from snakemake.io import expand, InputFiles, OutputFiles, Wildcards, Params, Log, Resources
from snakemake.io import apply_wildcards, is_flagged, not_iterable
from snakemake.exceptions import RuleException, IOFileException, WildcardError, InputFunctionException, WorkflowError
from snakemake.logging import logger


class Rule:
    def __init__(self, *args, lineno=None, snakefile=None):
        """
        Create a rule

        Arguments
        name -- the name of the rule
        """
        if len(args) == 2:
            name, workflow = args
            self.name = name
            self.workflow = workflow
            self.docstring = None
            self.message = None
            self._input = InputFiles()
            self._output = OutputFiles()
            self._params = Params()
            self._wildcard_constraints = dict()
            self.dependencies = dict()
            self.dynamic_output = set()
            self.dynamic_input = set()
            self.ancient_input = set() #RASMUS
            self.temp_output = set()
            self.protected_output = set()
            self.touch_output = set()
            self.subworkflow_input = dict()
            self.shadow_depth = None
            self.resources = dict(_cores=1, _nodes=1)
            self.priority = 0
            self.version = None
            self._log = Log()
            self._benchmark = None
            self.wildcard_names = set()
            self.lineno = lineno
            self.snakefile = snakefile
            self.run_func = None
            self.shellcmd = None
            self.script = None
            self.norun = False
        elif len(args) == 1:
            other = args[0]
            self.name = other.name
            self.workflow = other.workflow
            self.docstring = other.docstring
            self.message = other.message
            self._input = InputFiles(other._input)
            self._output = OutputFiles(other._output)
            self._params = Params(other._params)
            self._wildcard_constraints = dict(other._wildcard_constraints)
            self.dependencies = dict(other.dependencies)
            self.dynamic_output = set(other.dynamic_output)
            self.dynamic_input = set(other.dynamic_input)
            self.ancient_input = set(other.ancient_input) #RASMUS
            self.temp_output = set(other.temp_output)
            self.protected_output = set(other.protected_output)
            self.touch_output = set(other.touch_output)
            self.subworkflow_input = dict(other.subworkflow_input)
            self.shadow_depth = other.shadow_depth
            self.resources = other.resources
            self.priority = other.priority
            self.version = other.version
            self._log = other._log
            self._benchmark = other._benchmark
            self.wildcard_names = set(other.wildcard_names)
            self.lineno = other.lineno
            self.snakefile = other.snakefile
            self.run_func = other.run_func
            self.shellcmd = other.shellcmd
            self.script = other.script
            self.norun = other.norun

    def dynamic_branch(self, wildcards, input=True):
        def get_io(rule):
            return (rule.input, rule.dynamic_input) if input else (
                rule.output, rule.dynamic_output)

        def partially_expand(f, wildcards):
            """Expand the wildcards in f from the ones present in wildcards

            This is done by replacing all wildcard delimiters by `{{` or `}}`
            that are not in `wildcards.keys()`.
            """
            # perform the partial expansion from f's string representation
            s = str(f).replace('{', '{{').replace('}', '}}')
            for key in wildcards.keys():
                s = s.replace('{{{{{}}}}}'.format(key), '{{{}}}'.format(key))
            # build result
            anno_s = AnnotatedString(s)
            anno_s.flags = f.flags
            return IOFile(anno_s, f.rule)

        io, dynamic_io = get_io(self)

        branch = Rule(self)
        io_, dynamic_io_ = get_io(branch)

        expansion = defaultdict(list)
        for i, f in enumerate(io):
            if f in dynamic_io:
                f = partially_expand(f, wildcards)
                try:
                    for e in reversed(expand(f, zip, **wildcards)):
                        # need to clone the flags so intermediate
                        # dynamic remote file paths are expanded and
                        # removed appropriately
                        ioFile = IOFile(e, rule=branch)
                        ioFile.clone_flags(f)
                        expansion[i].append(ioFile)
                except KeyError:
                    return None

        # replace the dynamic files with the expanded files
        replacements = [(i, io[i], e)
                        for i, e in reversed(list(expansion.items()))]
        for i, old, exp in replacements:
            dynamic_io_.remove(old)
            io_.insert_items(i, exp)

        if not input:
            for i, old, exp in replacements:
                if old in branch.temp_output:
                    branch.temp_output.discard(old)
                    branch.temp_output.update(exp)
                if old in branch.protected_output:
                    branch.protected_output.discard(old)
                    branch.protected_output.update(exp)
                if old in branch.touch_output:
                    branch.touch_output.discard(old)
                    branch.touch_output.update(exp)

            branch.wildcard_names.clear()
            non_dynamic_wildcards = dict((name, values[0])
                                         for name, values in wildcards.items()
                                         if len(set(values)) == 1)
            # TODO have a look into how to concretize dependencies here
            branch._input, _, branch.dependencies = branch.expand_input(non_dynamic_wildcards)
            branch._output, _ = branch.expand_output(non_dynamic_wildcards)
            branch.resources = dict(branch.expand_resources(non_dynamic_wildcards, branch._input).items())
            branch._params = branch.expand_params(non_dynamic_wildcards, branch._input, branch.resources)
            branch._log = branch.expand_log(non_dynamic_wildcards)
            branch._benchmark = branch.expand_benchmark(non_dynamic_wildcards)
            return branch, non_dynamic_wildcards
        return branch

    def has_wildcards(self):
        """
        Return True if rule contains wildcards.
        """
        return bool(self.wildcard_names)

    @property
    def benchmark(self):
        return self._benchmark

    @benchmark.setter
    def benchmark(self, benchmark):
        self._benchmark = IOFile(benchmark, rule=self)

    @property
    def input(self):
        return self._input

    def set_input(self, *input, **kwinput):
        """
        Add a list of input files. Recursive lists are flattened.

        Arguments
        input -- the list of input files
        """
        for item in input:
            self._set_inoutput_item(item)
        for name, item in kwinput.items():
            self._set_inoutput_item(item, name=name)

    @property
    def output(self):
        return self._output

    @property
    def products(self):
        products = list(self.output)
        if self.benchmark:
            products.append(self.benchmark)
        return products

    def set_output(self, *output, **kwoutput):
        """
        Add a list of output files. Recursive lists are flattened.

        Arguments
        output -- the list of output files
        """
        for item in output:
            self._set_inoutput_item(item, output=True)
        for name, item in kwoutput.items():
            self._set_inoutput_item(item, output=True, name=name)

        for item in self.output:
            if self.dynamic_output and item not in self.dynamic_output:
                raise SyntaxError(
                    "A rule with dynamic output may not define any "
                    "non-dynamic output files.")
            wildcards = item.get_wildcard_names()
            if self.wildcard_names:
                if self.wildcard_names != wildcards:
                    raise SyntaxError("Not all output files of rule {} "
                                      "contain the same wildcards.".format(
                                          self.name))
            else:
                self.wildcard_names = wildcards

    def _set_inoutput_item(self, item, output=False, name=None):
        """
        Set an item to be input or output.

        Arguments
        item     -- the item
        inoutput -- either a Namedlist of input or output items
        name     -- an optional name for the item
        """
        inoutput = self.output if output else self.input
        if isinstance(item, str):
            # add the rule to the dependencies
            if isinstance(item, _IOFile):
                self.dependencies[item] = item.rule
            if output:
                if self.wildcard_constraints or self.workflow._wildcard_constraints:
                    try:
                        item = update_wildcard_constraints(
                            item, self.wildcard_constraints,
                            self.workflow._wildcard_constraints)
                    except ValueError as e:
                        raise IOFileException(
                            str(e),
                            snakefile=self.snakefile,
                            lineno=self.lineno)
            else:
                if contains_wildcard_constraints(item):
                    logger.warning(
                        "wildcard constraints in inputs are ignored")
            _item = IOFile(item, rule=self)
            if is_flagged(item, "temp"):
                if output:
                    self.temp_output.add(_item)
            if is_flagged(item, "protected"):
                if output:
                    self.protected_output.add(_item)
            #RASMUS
            if is_flagged(item, "ancient"):
                if input:
                    self.ancient_input.add(_item)
                else:
                    pass #RASMUS: SHOULD PRINT WARNING?
            if is_flagged(item, "touch"):
                if output:
                    self.touch_output.add(_item)
            if is_flagged(item, "dynamic"):
                if output:
                    self.dynamic_output.add(_item)
                else:
                    self.dynamic_input.add(_item)
            if is_flagged(item, "subworkflow"):
                if output:
                    raise SyntaxError(
                        "Only input files may refer to a subworkflow")
                else:
                    # record the workflow this item comes from
                    self.subworkflow_input[_item] = item.flags["subworkflow"]
            inoutput.append(_item)
            if name:
                inoutput.add_name(name)
        elif callable(item):
            if output:
                raise SyntaxError(
                    "Only input files can be specified as functions")
            inoutput.append(item)
            if name:
                inoutput.add_name(name)
        else:
            try:
                start = len(inoutput)
                for i in item:
                    self._set_inoutput_item(i, output=output)
                if name:
                    # if the list was named, make it accessible
                    inoutput.set_name(name, start, end=len(inoutput))
            except TypeError:
                raise SyntaxError(
                    "Input and output files have to be specified as strings or lists of strings.")

    @property
    def params(self):
        return self._params

    def set_params(self, *params, **kwparams):
        for item in params:
            self._set_params_item(item)
        for name, item in kwparams.items():
            self._set_params_item(item, name=name)

    def _set_params_item(self, item, name=None):
        self.params.append(item)
        if name:
            self.params.add_name(name)

    @property
    def wildcard_constraints(self):
        return self._wildcard_constraints

    def set_wildcard_constraints(self, **kwwildcard_constraints):
        self._wildcard_constraints.update(kwwildcard_constraints)

    @property
    def log(self):
        return self._log

    def set_log(self, *logs, **kwlogs):
        for item in logs:
            self._set_log_item(item)
        for name, item in kwlogs.items():
            self._set_log_item(item, name=name)

    def _set_log_item(self, item, name=None):
        if isinstance(item, str) or callable(item):
            self.log.append(IOFile(item,
                                   rule=self) if isinstance(item, str) else
                            item)
            if name:
                self.log.add_name(name)
        else:
            try:
                start = len(self.log)
                for i in item:
                    self._set_log_item(i)
                if name:
                    self.log.set_name(name, start, end=len(self.log))
            except TypeError:
                raise SyntaxError("Log files have to be specified as strings.")

    def check_wildcards(self, wildcards):
        missing_wildcards = self.wildcard_names - set(wildcards.keys())

        if missing_wildcards:
            raise RuleException(
                "Could not resolve wildcards in rule {}:\n{}".format(
                    self.name, "\n".join(self.wildcard_names)),
                lineno=self.lineno,
                snakefile=self.snakefile)

    def apply_input_function(self, func, wildcards, **aux_params):
        sig = inspect.signature(func)
        _aux_params = {k: v for k, v in aux_params.items() if k in sig.parameters}
        try:
            value = func(Wildcards(fromdict=wildcards), **_aux_params)
        except (Exception, BaseException) as e:
            raise InputFunctionException(e, rule=self, wildcards=wildcards)
        return value

    def _apply_wildcards(self, newitems, olditems, wildcards,
                         concretize=apply_wildcards,
                         check_return_type=True,
                         mapping=None,
                         no_flattening=False,
                         aux_params=None):
        if aux_params is None:
            aux_params = dict()
        for name, item in olditems.allitems():
            start = len(newitems)
            is_iterable = True

            if callable(item):
                item = self.apply_input_function(item, wildcards, **aux_params)

            if not_iterable(item) or no_flattening:
                item = [item]
                is_iterable = False
            for item_ in item:
                if check_return_type and not isinstance(item_, str):
                    raise WorkflowError("Function did not return str or list "
                                        "of str.", rule=self)
                concrete = concretize(item_, wildcards)
                newitems.append(concrete)
                if mapping is not None:
                    mapping[concrete] = item_

            if name:
                newitems.set_name(
                    name, start,
                    end=len(newitems) if is_iterable else None)

    def expand_input(self, wildcards):
        def concretize_iofile(f, wildcards):
            if not isinstance(f, _IOFile):
                return IOFile(f, rule=self)
            else:
                return f.apply_wildcards(wildcards,
                                         fill_missing=f in self.dynamic_input,
                                         fail_dynamic=self.dynamic_output)

        input = InputFiles()
        mapping = dict()
        try:
            self._apply_wildcards(input, self.input, wildcards,
                                  concretize=concretize_iofile,
                                  mapping=mapping)
        except WildcardError as e:
            raise WorkflowError(
                "Wildcards in input files cannot be "
                "determined from output files:",
                str(e), rule=self)

        if self.dependencies:
            dependencies = {
                f: self.dependencies[f_]
                for f, f_ in mapping.items() if f_ in self.dependencies
            }
            if None in self.dependencies:
                dependencies[None] = self.dependencies[None]
        else:
            dependencies = self.dependencies

        for f in input:
            f.check()

        return input, mapping, dependencies

    def expand_params(self, wildcards, input, resources):
        # TODO add output
        def concretize_param(p, wildcards):
            if isinstance(p, str):
                return apply_wildcards(p, wildcards)
            return p

        params = Params()
        try:
            #When applying wildcards to params, the return type need not be
            #a string, so the check is disabled.
            self._apply_wildcards(params, self.params, wildcards,
                                  concretize=concretize_param,
                                  check_return_type=False,
                                  no_flattening=True,
                                  aux_params={"input": input,
                                              "resources": resources})
        except WildcardError as e:
            raise WorkflowError(
                "Wildcards in params cannot be "
                "determined from output files:",
                str(e), rule=self)
        return params


    def expand_output(self, wildcards):
        output = OutputFiles(o.apply_wildcards(wildcards)
                             for o in self.output)
        output.take_names(self.output.get_names())
        mapping = {f: f_ for f, f_ in zip(output, self.output)}

        for f in output:
            f.check()

        return output, mapping


    def expand_log(self, wildcards):
        def concretize_logfile(f, wildcards):
            if not isinstance(f, _IOFile):
                return IOFile(f, rule=self)
            else:
                return f.apply_wildcards(wildcards,
                                         fill_missing=False,
                                         fail_dynamic=self.dynamic_output)

        log = Log()

        try:
            self._apply_wildcards(log,
                                  self.log,
                                  wildcards,
                                  concretize=concretize_logfile)
        except WildcardError as e:
            raise WorkflowError(
                "Wildcards in log files cannot be "
                "determined from output files:",
                str(e), rule=self)

        for f in log:
            f.check()

        return log

    def expand_benchmark(self, wildcards):
        try:
            benchmark = self.benchmark.apply_wildcards(
                wildcards) if self.benchmark else None
        except WildcardError as e:
            raise WorkflowError(
                "Wildcards in benchmark file cannot be "
                "determined from output files:",
                str(e), rule=self)

        if benchmark is not None:
            benchmark.check()

        return benchmark

    def expand_resources(self, wildcards, input):
        resources = dict()
        for name, res in self.resources.items():
            if callable(res):
                res = self.apply_input_function(res,
                                                wildcards,
                                                input=input)
                if not isinstance(res, int):
                    raise WorkflowError("Resources function did not return int.")
            res = min(self.workflow.global_resources.get(name, res), res)
            resources[name] = res
        resources = Resources(fromdict=resources)
        return resources

    def is_producer(self, requested_output):
        """
        Returns True if this rule is a producer of the requested output.
        """
        try:
            for o in self.products:
                if o.match(requested_output):
                    return True
            return False
        except sre_constants.error as ex:
            raise IOFileException("{} in wildcard statement".format(ex),
                                  snakefile=self.snakefile,
                                  lineno=self.lineno)
        except ValueError as ex:
            raise IOFileException("{}".format(ex),
                                  snakefile=self.snakefile,
                                  lineno=self.lineno)

    def get_wildcards(self, requested_output):
        """
        Update the given wildcard dictionary by matching regular expression
        output files to the requested concrete ones.

        Arguments
        wildcards -- a dictionary of wildcards
        requested_output -- a concrete filepath
        """
        if requested_output is None:
            return dict()
        bestmatchlen = 0
        bestmatch = None

        for o in self.products:
            match = o.match(requested_output)
            if match:
                l = self.get_wildcard_len(match.groupdict())
                if not bestmatch or bestmatchlen > l:
                    bestmatch = match.groupdict()
                    bestmatchlen = l
        self.check_wildcards(bestmatch)
        return bestmatch

    @staticmethod
    def get_wildcard_len(wildcards):
        """
        Return the length of the given wildcard values.

        Arguments
        wildcards -- a dict of wildcards
        """
        return sum(map(len, wildcards.values()))

    def __lt__(self, rule):
        comp = self.workflow._ruleorder.compare(self, rule)
        return comp < 0

    def __gt__(self, rule):
        comp = self.workflow._ruleorder.compare(self, rule)
        return comp > 0

    def __str__(self):
        return self.name

    def __hash__(self):
        return self.name.__hash__()

    def __eq__(self, other):
        return self.name == other.name and self.output == other.output


class Ruleorder:
    def __init__(self):
        self.order = list()

    def add(self, *rulenames):
        """
        Records the order of given rules as rule1 > rule2 > rule3, ...
        """
        self.order.append(list(rulenames))

    def compare(self, rule1, rule2):
        """
        Return whether rule2 has a higher priority than rule1.
        """
        # try the last clause first,
        # i.e. clauses added later overwrite those before.
        for clause in reversed(self.order):
            try:
                i = clause.index(rule1.name)
                j = clause.index(rule2.name)
                # rules with higher priority should have a smaller index
                comp = j - i
                if comp < 0:
                    comp = -1
                elif comp > 0:
                    comp = 1
                return comp
            except ValueError:
                pass

        # if not ruleorder given, prefer rule without wildcards
        wildcard_cmp = rule2.has_wildcards() - rule1.has_wildcards()
        if wildcard_cmp != 0:
            return wildcard_cmp

        return 0

    def __iter__(self):
        return self.order.__iter__()
