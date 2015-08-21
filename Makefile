
all:
	make -C ./common
	cd blaze; mvn package
	make -C ./manager/src

examples:
	make -C examples
