
set terminal pngcairo enhanced color size 1024,768

summaries = "100k"
threshold = "100"

title_size_major = 20
title_size_minor = 14

title_suffix = sprintf("\t\t{/=%d Category \"".system('basename $(pwd)')."\", %s Summaries, %s Threshold}", title_size_minor, summaries, threshold)

set key off
set xtics nomirror rotate by 60 right
set style line 1 linecolor rgb 'black'

x_max = system('wc -l table.dat')
set xrange [-1:x_max+1]

xskip = 8

set xlabel '{/=14 Host}'

# Wall Time
set title sprintf("{/=%d Wall Time vs. Host}%s", title_size_major, title_suffix)
set ylabel '{/=14 Wall Time (hr)}'
set output 'wall_time.png'
plot 'table.dat' using 0:($3/3600):xticlabels(int(column(0)) % xskip == 0 ? strcol(1) : '') with points pt 7 lc rgb 'black', \
     '' using 0:($3/3600):($4/3600) with yerrorbars ls 1

# CPU Time
set title sprintf("{/=%d CPU Time vs. Host}%s", title_size_major, title_suffix)
set ylabel '{/=14 CPU Time (hr)}'
set output 'cpu_time.png'
plot 'table.dat' using 0:($5/3600):xticlabels(int(column(0)) % xskip == 0 ? strcol(1) : '') with points pt 7 lc rgb 'black', \
     '' using 0:($5/3600):($6/3600) with yerrorbars ls 1

# CPU Time/Wall Time
set title sprintf("{/=%d (CPU Time per Wall Time) vs. Host}%s", title_size_major, title_suffix)
set ylabel '{/=14 CPU Time / Wall Time}'
set output 'cpu_per_wall.png'
plot 'table.dat' using 0:($5/$3):xticlabels(int(column(0)) % xskip == 0 ? strcol(1) : '') with points pt 7 lc rgb 'black', \
     '' using 0:($5/$3):(abs($6/$5) + abs($4/$3)) with yerrorbars ls 1
