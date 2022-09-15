# 
# designate compiler
CC := clang
CXX := clang++

CFLAGS := -Werror

# PHONY
.PHONY: all clean

# target
all: db

# all objects
objects := main.o


db: $(objects)
	$(CC) $(CFLAGS) -o $@ $(objects)


clean:
	rm db $(objects)
