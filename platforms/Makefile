SUBDIRS = nv_opencl

all: $(SUBDIRS)

clean: 
	for dir in $(SUBDIRS); do \
			$(MAKE) -C $$dir $@; \
	done

$(SUBDIRS): 
	$(MAKE) -C $@

.PHONY: all clean $(SUBDIRS)
