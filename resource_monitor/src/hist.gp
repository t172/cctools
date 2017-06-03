
set terminal pngcairo enhanced size 576,1024
set key off
set output 'wall_time-hist.png'
hosts = system("sed '1d;s/ .*$//' wall_time.dat")

set style line 1 lc rgb 'grey80'
set style fill transparent solid 0.9 border lc rgb 'black'

set title 'Wall Time vs. Host'
set xlabel 'Wall Time (hr)'
set ylabel 'Host'

vspread = 2

set format y ''
do for [i=1:(words(hosts))] {
lbl = system("echo ".word(hosts, i)." | sed 's/\.crc\.nd\.edu//'");
if ( i % 7 == 0 ) { set ytics add ( (lbl) (-vspread*i) ) }
}

# set format y ''
# set for [i=1:(words(hosts))] ytics add ( (word(hosts, i)) (-vspread*i) )

# Blocky square histogram
#tweak(file) = sprintf("<awk '$1==\"NAN\"{next}NR==2{x=$1;y=0}NR>=2{print $1,y;print $1,$2;x=$1;y=$2}END{print x,0}' %s", file)

# Triangle wave
tweak(file) = sprintf("<awk '$1==\"NAN\"{next}NR==2{x=$1;y=0}NR>=2{print ($1+x)/2,y;print $1,(y+$2)/2;x=$1;y=$2}END{print x,0}' %s", file)

plot for [i=1:(words(hosts))] (tweak(word(hosts, i).'.hist')) using ($1/3600):($2 - vspread*i) smooth csplines with filledcurves ls 1 notitle
