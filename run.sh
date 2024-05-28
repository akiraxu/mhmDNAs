#!/bin/sh
npm i
node --max_old_space_size=16384 --expose-gc web