all: common accrdd nam

common: 
	make -C ./common

accrdd: common
	cd accrdd; mvn package; mvn install

nam: common
	make -C ./manager/src
	make -C ./platforms

examples:
	make -C examples

.PHONY: all common accrdd nam examples
