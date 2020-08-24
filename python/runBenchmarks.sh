 #!/bin/sh
function prepend() { while read line; do echo "${1}${line}"; done; }
for i in {1..5}
do
	{ python3 runGenerators.py "run$i/" | prepend "[GENERATOR] " & python3 runEngines.py "run$i/" | prepend "[ENGINE] "; }
done

