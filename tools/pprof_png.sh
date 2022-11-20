rm -f tools/profiler/*.png
for file in tools/profiler/*.pprof; do go tool pprof -png "$1" "$file"; done
