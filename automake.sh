#!/bin/bash

autoreconf -i
mkdir -p autotools
aclocal
autoheader
autoconf
automake
