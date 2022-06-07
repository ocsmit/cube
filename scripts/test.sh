#!/bin/bash

cd "$(.. "$0")"
pytest  -W ignore::DeprecationWarning -vv

