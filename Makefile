.PHONY: all glide-update

all:
	make -C pkg
	make -C cmd

glide-update:
	glide update -v
