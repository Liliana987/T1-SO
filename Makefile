CC = gcc
CFLAGS = -Wall -Wextra -std=c17
TARGET = planificador

all: $(TARGET)

$(TARGET): main.c
	$(CC) $(CFLAGS) -o $(TARGET) main.c

clean:
	rm -f $(TARGET)