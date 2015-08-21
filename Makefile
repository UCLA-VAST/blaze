
all:
	make -C ./common
	cd accrdd; mvn package
	make -C ./manager/src

examples:
	make -C examples
