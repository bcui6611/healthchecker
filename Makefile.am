COUCHBASE_PYTHON_LIB = ../couchbase-python-client/couchbase

default:

pythonlibdir=$(libdir)/python

pythonlib_SCRIPTS= healthChecker

PYTHON_TOOLS= wrapper/healthChecker

${PYTHON_TOOLS}: wrapper/wrapper
	cp $< $@

CLEANFILES = ${PYTHON_TOOLS}
bin_SCRIPTS = ${PYTHON_TOOLS}

EXTRA_DIST = $(pythonlib_SCRIPTS)

install-data-hook:
	cp -rf $(COUCHBASE_PYTHON_LIB) $(pythonlibdir)


