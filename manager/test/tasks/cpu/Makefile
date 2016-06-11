#include common.mk

SUBDIRS = loopBack \
					arrayTest

all: $(SUBDIRS)

clean: 
	for dir in $(SUBDIRS); do \
			$(MAKE) -C $$dir $@; \
	done

$(SUBDIRS): 
	$(MAKE) -C $@

.PHONY: all clean $(SUBDIRS)
