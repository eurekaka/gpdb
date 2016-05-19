subdir=src/backend/cdb/dispatcher
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

TARGETS=cdbdisp_query \
		cdbdispatchresult

include $(top_builddir)/src/backend/mock.mk

# dispatcher: \
# 	dispatcher.o \
# 	$(MOCK_DIR)/backend/cdb/cdbgang_mock.o \
# 	$(OBJFILES) $(CMOCKERY_OBJS) $(MOCK_OBJS)
# 	$(CC) $(CFLAGS) $(LDFLAGS) \
# 		$(call BACKEND_OBJS, $(patsubst $(MOCK_DIR)/%_mock.o,$(top_builddir)/src/%.o, $^)) \
# 		$(filter-out %/objfiles.txt, $^) $(MOCK_LIBS) -o $@

# clean:
# 	rm -f dispatcher.o dispatcher;
