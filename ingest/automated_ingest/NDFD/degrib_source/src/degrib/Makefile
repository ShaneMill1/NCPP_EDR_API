SHELL = /bin/sh

top_srcdir = ..
TCL_PREFIX = /home/shane.mill/miniconda3/envs/shane-test
TK_PREFIX = /home/shane.mill/miniconda3/envs/shane-test
TCL_VERSION = 8.6

CC = gcc
STRIP = /usr/bin/strip
STRIP_FLAGS = -s
STD_FLAGS = -g -O2 -Wall -fsigned-char -pedantic
DWML_FLAGS = -g -O2 -Wall -fsigned-char -pedantic
EXEEXT = 
LD_FLAGS = 
TCL_LDFLAGS = ${LD_FLAGS} 
TK_LDFLAGS = ${LD_FLAGS} 

############################
# XML Dependence
XML_DEF = -D_DWML_
XML_INC = -I../degrib -I../libxml/include -I../dwmllib
XML_LIB = -L../dwmllib -ldwml -L../libxml/.libs/ -lxml2
XML_LIBDEP = ../dwmllib/libdwml.a ../libxml/.libs/libxml2.a
XML_CLEAN = (cd ../dwmllib && make -f makefile clean)
############################

STD_DEF = -DSIZEOF_LONG_INT=8    -D_LINUX_  $(XML_DEF)
STUB_DEF = -DUSE_TCL_STUBS 
STD_INC =  -I../emapf-c -I../mdl_g2c -I../libaat -I../netcdf/libsrc -I../gd -I../zlib/contrib/minizip -I../zlib $(XML_INC)
TCL_INC = -I$(TCL_PREFIX)/include
TK_INC = -I$(TK_PREFIX)/include
GUI_INC = $(TCL_INC) $(TK_INC) $(STD_INC)

CLOCK_LIB =  -L/usr/lib -lm

# The libaat dependence is just if we have debug on.
DP_LIB = -L../libaat -laat  -L../emapf-c/ -lemapf $(XML_LIB) -L../zlib/contrib/minizip -lminizip -L../zlib -lz -L/usr/lib -lm

DRAWSHP_LIB =  -L../gd/ -lgd -L../libpng -lpng -L../zlib -lz \
          -L../emapf-c/ -lemapf -L/usr/lib -lm
# XML_LIB has to come before zlib.
# Found that gd has to come before tk (on some linux machines)
STD_LIB1 = $(XML_LIB) -L../mdl_g2c -lmdl_g2c -L../emapf-c/ -lemapf \
          -L../netcdf/libsrc/ -lnetcdf \
          -L../jpeg2000/src/libjasper/jpc/.libs/ -ljpc \
          -L../jpeg2000/src/libjasper/base/.libs/ -lbase \
          -L../libpng -lpng -L../zlib/contrib/minizip -lminizip -L../zlib -lz \
          -L../libaat -laat  \
          -L/usr/lib -lm
STD_LIB = -L../gd -lgd $(STD_LIB1)

TCL_LIB = -L/home/shane.mill/miniconda3/envs/shane-test/lib -ltcl8.6 -ltclstub8.6
TK_LIB = -L/home/shane.mill/miniconda3/envs/shane-test/lib -ltk8.6 -ltkstub8.6
TCLSTUB_LIB=-L$(TCL_PREFIX)/lib -ltclstub$(TCL_VERSION) 
X_LIB = 
DYNAMIC_LIB = -ldl

PRJ_DLL = libdegrib$(TCL_VERSION).so
GUI_LIB1 = -L../gd -lgd $(TK_LIB) $(TCL_LIB) $(X_LIB) $(DYNAMIC_LIB) $(STD_LIB1)
GUI_LIB2 = -L../gd -lgd $(TK_LIB) $(TCL_LIB) $(X_LIB) $(DYNAMIC_LIB) $(STD_LIB1)
STUB_LIB = -L../gd -lgd $(TCLSTUB_LIB) $(X_LIB) $(DYNAMIC_LIB) $(STD_LIB1)

CLOCK_NAME = clock
DP_NAME = degrib_DP
DRAWSHP_NAME = drawshp
PRJ_NAME = degrib
TCL_NAME = tcldegrib
TK_NAME = tkdegrib
PRJ_A = libdegrib$(TCL_VERSION).a

CFLAGS = $(STD_FLAGS) $(STD_DEF) $(STD_INC)
GUIFLAGS = $(STD_FLAGS) $(STD_DEF) $(GUI_INC)
DWMLFLAGS = $(DWML_FLAGS) $(STD_DEF) $(STD_INC)

############################
# FILES
############################
C_OBJECTS = myutil.o \
            clock.o \
            mymapf.o \
            myerror.o \
            myassert.o \
            tendian.o \
            scan.o \
            metaname.o \
            metaparse.o \
            hazard.o \
            weather.o \
            grib1tab.o \
            metaprint.o \
            writeflt.o \
            chain.o \
            writecsv.o \
            writeshp.o \
            writegra.o \
            writenc.o \
            readnc.o \
            interp.o \
            inventory.o \
            probe.o \
            userparse.o \
            tdlpack.o \
            degrib1.o \
            degrib2.o \
            pack.o \
            cube.o \
            drawgrib.o \
            commands.o \
            database.o \
            mapini.o \
            drawlib.o \
            genprobe.o \
            solar.o \
            grpprobe.o \
            sector.o \
            writekml.o \
            split.o \
            myzip.o

H_SOURCES = type.h \
            myutil.h \
            clock.h \
            myerror.h \
            myassert.h \
            mymapf.h \
            tendian.h \
            scan.h \
            meta.h \
            metaname.h \
            hazard.h \
            weather.h \
            chain.h \
            write.h \
            interp.h \
            inventory.h \
            probe.h \
            userparse.h \
            tdlpack.h \
            degrib1.h \
            degrib2.h \
            pack.h \
            drawgrib.h \
            commands.h \
            database.h \
            mapini.h \
            drawlib.h \
            genprobe.h \
            solar.h \
            grpprobe.h \
            sector.h \
            split.h \
            myzip.h  

CLOCK_OBJECTS = myassert.o \
            myutil.o \
            myerror.o \
            clock.o

DP_OBJECTS = myutil.o \
            clock.o \
            mymapf.o \
            myerror.o \
            myassert.o \
            tendian.o \
            hazard.o \
            weather.o \
            interp.o \
            userparse.o \
            database.o \
            solar.o \
            grpprobe.o \
            sector.o \
            dpgenprobe.o

DRAWSHP_OBJECTS = myassert.o \
            myutil.o \
            tendian.o \
            mapini.o \
            drawlib.o

GUI_OBJECTS = $(C_OBJECTS) \
            tcldegrib.obj

STUB_OBJECTS = $(C_OBJECTS) \
            tcldegrib.o

C_MAIN = cstart.c

GUI_MAIN = tclstart.c

CLOCK_MAIN = clockstart.c

DP_MAIN = dpstart.c

DRAWSHP_MAIN = drawshp.c

LIB_DEPENDS = ../emapf-c/libemapf.a ../mdl_g2c/libmdl_g2c.a ../libaat/libaat.a \
            ../libpng/libpng.a ../zlib/libz.a ../zlib/contrib/minizip/libminizip.a \
            ../jpeg2000/src/libjasper/jpc/.libs/libjpc.a \
            ../jpeg2000/src/libjasper/base/.libs/libbase.a \
            ../netcdf/libsrc/libnetcdf.a ../gd/libgd.a $(XML_LIBDEP) 

############################
# TARGETS
############################
#all: $(PRJ_NAME) $(CLOCK_NAME) $(DP_NAME) $(DRAWSHP_NAME) $(TCL_NAME) $(TK_NAME) $(PRJ_DLL)
all: $(PRJ_NAME) $(CLOCK_NAME) $(DP_NAME) $(DRAWSHP_NAME) $(TCL_NAME) $(TK_NAME)

# In order for PRJ_DLL to work we need to compile with STUB_LIB / STUB_DEF
$(PRJ_DLL): $(STUB_OBJECTS) $(LIB_DEPENDS) $(H_SOURCES)
#	$(CC) -shared $(STUB_OBJECTS) $(STUB_LIB) -o $(PRJ_DLL) \
#         -Wl,--output-def,$(PRJ_NAME).def,--out-implib,$(PRJ_A)
#	$(CC) -shared $(STUB_OBJECTS) $(STUB_LIB) -o $(PRJ_DLL)
	$(CC) -shared $(STUB_OBJECTS) $(STUB_LIB) -o $(PRJ_DLL)
	$(STRIP) $(STRIP_FLAGS) $(PRJ_DLL)

$(PRJ_NAME): $(C_OBJECTS) $(C_MAIN) $(LIB_DEPENDS) $(H_SOURCES)
	$(CC) $(C_MAIN) $(CFLAGS) $(LD_FLAGS) $(C_OBJECTS) $(STD_LIB) -o $(PRJ_NAME)
	$(STRIP) $(STRIP_FLAGS) $(PRJ_NAME)$(EXEEXT)

$(CLOCK_NAME): $(CLOCK_MAIN) $(CLOCK_OBJECTS) $(H_SOURCES)
	$(CC) $(CLOCK_MAIN) $(CFLAGS) $(CLOCK_OBJECTS) $(CLOCK_LIB) -o $(CLOCK_NAME)
	$(STRIP) $(STRIP_FLAGS) $(CLOCK_NAME)$(EXEEXT)

$(DP_NAME): $(DP_OBJECTS) $(DP_MAIN) $(LIB_DEPENDS) $(H_SOURCES)
	$(CC) $(DP_MAIN) $(CFLAGS) $(LD_FLAGS) $(DP_OBJECTS) $(DP_LIB) -o $(DP_NAME)
	$(STRIP) $(STRIP_FLAGS) $(DP_NAME)$(EXEEXT)

$(DRAWSHP_NAME): $(DRAWSHP_MAIN) $(DRAWSHP_OBJECTS) $(H_SOURCES)
	$(CC) $(DRAWSHP_MAIN) $(CFLAGS) $(DRAWSHP_OBJECTS) $(DRAWSHP_LIB) -o $(DRAWSHP_NAME)
	$(STRIP) $(STRIP_FLAGS) $(DRAWSHP_NAME)$(EXEEXT)

$(TK_NAME): $(GUI_OBJECTS) $(GUI_MAIN) $(LIB_DEPENDS) $(H_SOURCES)
	$(CC) $(GUI_MAIN) $(GUIFLAGS) $(TK_LDFLAGS) $(GUI_OBJECTS) $(GUI_LIB2) -o $(TK_NAME)
	$(STRIP) $(STRIP_FLAGS) $(TK_NAME)$(EXEEXT)

$(TCL_NAME): $(GUI_OBJECTS) $(GUI_MAIN) $(LIB_DEPENDS) $(H_SOURCES)
	$(CC) $(GUI_MAIN) $(GUIFLAGS) -DNO_TK $(TCL_LDFLAGS) $(GUI_OBJECTS) $(GUI_LIB1) -o $(TCL_NAME)
	$(STRIP) $(STRIP_FLAGS) $(TCL_NAME)$(EXEEXT)

# Note: Absence of TCL_NAME and TK_NAME intentional (so degrib can be built
# and installed without Tcl/Tk).
install: $(PRJ_NAME) $(CLOCK_NAME) $(DP_NAME) $(DRAWSHP_NAME)
	cp $(PRJ_NAME)$(EXEEXT) ../../bin
	cp $(CLOCK_NAME)$(EXEEXT) ../../bin
	cp $(DP_NAME)$(EXEEXT) ../../bin
	cp $(DRAWSHP_NAME)$(EXEEXT) ../../bin
	cp $(TCL_NAME)$(EXEEXT) ../../bin
	cp $(TK_NAME)$(EXEEXT) ../../bin

clean:
	rm -f *.o *.bak *.BAK *.obj
	rm -f $(PRJ_NAME)$(EXEEXT)
	rm -f $(CLOCK_NAME)$(EXEEXT)
	rm -f $(DP_NAME)$(EXEEXT)
	rm -f $(DRAWSHP_NAME)$(EXEEXT)
	rm -f $(TCL_NAME)$(EXEEXT)
	rm -f $(TK_NAME)$(EXEEXT)
	$(XML_CLEAN)

distclean: clean
	rm -r -f Makefile

../dwmllib/libdwml.a:
	@echo "-----------------------------"
	@echo "Attempting to compile dwmllib..."
	(cd ../dwmllib && export CC='$(CC)' && export CFLAGS='$(DWMLFLAGS)' && make -f makefile ARFLAGS=' -ruv')
	@echo "Finished with dwmllib..."
	@echo "-----------------------------"

############################
# SUFFIXES
############################
tcldegrib.o: tcldegrib.c tcldegrib.h $(H_SOURCES)
	$(CC) -c $(STUB_DEF) $(GUIFLAGS)  tcldegrib.c

tcldegrib.obj: tcldegrib.c tcldegrib.h $(H_SOURCES)
	$(CC) -c $(GUIFLAGS) -o tcldegrib.obj tcldegrib.c

dpgenprobe.o : $(H_SOURCES) genprobe.c
	$(CC) -c $(CFLAGS) -DDP_ONLY genprobe.c -o dpgenprobe.o

.c.o : $(H_SOURCES)
	$(CC) -c $(CFLAGS) $<

