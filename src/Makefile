all: cli_multidb

CFLAGS=`pkg-config --cflags glib-2.0`

cli_multidb: cli_multidb.o libmultidb.dylib
	$(CC) -g -o cli_multidb cli_multidb.o -L. -lmultidb `pkg-config --libs glib-2.0`

libmultidb.dylib: libmultidb.c
	# $(CC) -g -shared -Wl,-soname,libmultidb.so -o libmultidb.so.1.0.0 libmultidb.o
	# ldconfig -N .
	$(CC) -g $(CFLAGS) -dynamiclib -o libmultidb.dylib libmultidb.c `pkg-config --libs glib-2.0`

# libmultidb.o:
#	$(CC) -g $(CFLAGS) -std=c99 -c libmultidb.c -fPIC

clean:
	rm -f cli_multidb.o cli_multidb.o libmultidb.o
	rm -f libmultidb.so libmultidb.so.1 libmultidb.so.1.0.0*
	rm -f cli_multidb
	rm -f libmultidb.dylib
	rm -f libmultidb.dylib.dSYM/Contents/Resources/DWARF/libmultidb.dylib
	rm -f libmultidb.dylib.dSYM/Contents/Info.plist
	rmdir libmultidb.dylib.dSYM/Contents/Resources/DWARF
	rmdir libmultidb.dylib.dSYM/Contents/Resources
	rmdir libmultidb.dylib.dSYM/Contents
	rmdir libmultidb.dylib.dSYM
	
### gcc -fPIC -g -c -Wall a.c
### gcc -fPIC -g -c -Wall b.c
### gcc -shared -Wl,-soname,libmystuff.so.1 \
###     -o libmystuff.so.1.0.1 a.o b.o -lc
