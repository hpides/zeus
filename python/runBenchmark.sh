 #!/bin/sh
function prepend() { while read line; do echo "${1}${line}"; done; }
for i in $(seq $2)
do
	{ python3 runGenerators.py "run$i/"  "$1"| prepend "[GENERATOR] " & python3 runEngines.py "run$i/" "$1" | prepend "[ENGINE] "; }
done

