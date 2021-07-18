
all:
	cd system; make; cd -
	cd debug; make; cd -
	mv system/runaimdb ./
	cd benchmark; make; cd -

clean:
	cd system; make clean; cd -
	cd debug; make clean; cd -
	rm runaimdb
	cd benchmark; make clean; cd -
