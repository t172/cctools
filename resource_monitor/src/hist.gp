
set terminal pngcairo enhanced size 640,1024
set key off
set output 'wall_time-hist.png'
hosts = system("sed '1d;s/ .*$//' wall_time.dat")

field = 3

set multiplot layout 2,1 title 'Wall Time vs. Host' font ',14'

set style line 1 lc rgb 'grey80'
set style line 2 lc rgb 'black'
set style fill transparent solid 0.9 border lc rgb 'black'

all_yscale = 1
yscale = 1
vspread = 1.5

# set format y ''
# set for [i=1:(words(hosts))] ytics add ( (word(hosts, i)) (-vspread*i) )

# Blocky square histogram
#tweak(file) = sprintf("<awk -v 'c=%d' '$1==\"NAN\"{next}NR==2{x=$c;y=0}NR>=2{print $c,y;print $c,$(c+1);x=$c;y=$(c+1)}END{print x,0}' '%s'", field, file)

# Triangularized
tweak(file) = sprintf("<awk -v 'c=%d' '$c==\"NAN\"{next}NR==2{x=$c;y=0}NR>=2{print ($c+x)/2,y;print $c,(y+$(c+1))/2;x=$c;y=$(c+1)}END{print x,0}' '%s'", field, file)
#tweak(file) = sprintf("<awk -v 'c=%d' '$c==\"NAN\"{next}NR==2{x=$c;y=0}NR>=2{print ($c+x)/2,y;print $c,(y+$(c+1))/2;x=$c;y=$(c+1)}END{print x,0}' '%s'", field, file)

#tweak(file) = sprintf("<awk -v 'c=%d' '$c==\"NAN\"{next}NR==2{x=$c;y=0}NR>=2{print ($c+x)/2,y;print $c,(y+$(c+1))/2;x=$c;y=$(c+1)}END{print x,0}' '%s'", field, file)

#tweak(file) = file

set lmargin at screen 0.2

set size 1,0.3
set origin 0,0.7
set bmargin 0
set tmargin 2
unset xlabel
unset ytics
set format x ''
set ylabel 'All'
plot tweak('(all).hist') using ($1/3600):(all_yscale*$2) with filledcurves ls 2 notitle

set size 1,0.7
set origin 0,0
set bmargin 3.5
set tmargin 0
set xlabel 'Wall Time (hr)'
set format x '%g'
#set ylabel 'Host' offset 3,0
unset ylabel
set format y ''
do for [i=1:(words(hosts))] {
lbl = system("echo ".word(hosts, i)." | sed 's/\.crc\.nd\.edu//;s/\.nd\.edu//'")
if ( i % 9 == 0 ) { set ytics add ( (lbl) (-vspread*i) ) }
}
set ytics font ",9"
set yrange [-vspread*(1.05*words(hosts)):]

plot for [i=1:(words(hosts))] (tweak(word(hosts, i).'.hist')) using ($1/3600):(yscale*$2 - vspread*i) with filledcurves ls 1 notitle

unset multiplot
