#!/bin/sh
function prepend() { while read line; do echo "${1}${line}"; done; }

{ python3 runGenerators.py | prepend "[GENERATOR] " & python3 runEngines.py | prepend "[ENGINE] "; }
