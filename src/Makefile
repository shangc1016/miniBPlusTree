# 
# designate compiler
CC := gcc
CXX := g++

CFLAGS := -Werror -O0 -g

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
