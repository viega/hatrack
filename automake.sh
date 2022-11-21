#!/bin/bash

mkdir -p autotools
aclocal
autoheader
autoconf
automake
