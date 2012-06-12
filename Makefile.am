TMP_DIR = ./tmp
TMP_VER = $(TMP_DIR)/version_num.tmp

CHEETAH_LIB = ./cheetah
JSON_LIB = ./simplejson

default:

pythonlibdir=$(libdir)/python
pythonlib_DATA = \
    analyzer.py \
    bucket_stats.py \
    buckets.py \
    cluster_stats.py \
    collector.py \
    diskqueue_stats.py \
    info.py \
    listservers.py \
    mc_bin_client.py \
    memcacheConstants.py \
    node_stats.py \
    restclient.py \
    stats_buffer.py \
    usage.py \
    util_cli.py

pythonlib_SCRIPTS= healthChecker

PYTHON_TOOLS= wrapper/healthChecker

${PYTHON_TOOLS}: wrapper/wrapper
	cp $< $@

CLEANFILES = ${PYTHON_TOOLS}
bin_SCRIPTS = ${PYTHON_TOOLS}

EXTRA_DIST = $(pythonlib_DATA) $(pythonlib_SCRIPTS) LICENSE

clean-local:
	rm -f membase*tar.gz
	rm -rf $(TMP_DIR)

install-data-hook:
	cp -rf $(CHEETAH_LIB) $(JSON_LIB) $(pythonlibdir)

bdist:
	test -d $(TMP_DIR) || mkdir $(TMP_DIR)
	git describe | sed s/-/_/g > $(TMP_VER)
	rm -f ./healthChecker_*.tar.gz
	rm -rf $(TMP_DIR)/healthChecker
	mkdir -p $(TMP_DIR)/healthChecker/simplejson
	cp healthChecker *.py  $(TMP_DIR)/healthChecker
	cp -rf $(CHEETAH_LIB) $(JSON_LIB) LICENSE $(TMP_DIR)/healthChecker
	(cd $(TMP_DIR); tar cf - healthChecker) | gzip -9 > healtherChecker_`cat $(TMP_VER)`-`uname -s`.`uname -m`.tar.gz
	echo created healtherChecker_`cat $(TMP_VER)`-`uname -s`.`uname -m`.tar.gz
	rm -rf $(TMP_DIR)

