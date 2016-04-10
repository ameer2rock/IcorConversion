#!/usr/bin/perl -w

use Image::Magick;

sub extract_tifs {
	my $source_file = shift;
	my $destination_file = shift;
	my $offset = shift;
	my $length = shift;

	my $input_fh;
	my $output_fh;
	my $tif_data;

	chomp $destination_file;
	open($input_fh,'<',$source_file) or return 0;
        print "one";
	seek($input_fh,$offset,0);
	read($input_fh,$tif_data,$length) or return 0;
        print "two";
	close $input_fh or return 0;
        print "three";
	# un buffer output
	select STDOUT; $| = 1;
	open($output_fh,'>',$destination_file) or return 0;
        print "four";
	print $output_fh $tif_data;
        print "4.5";
	close $output_fh or return 0;
        print "five";

	# need to use lib tif to check outfile validity
	return 1;
}
$copy_source="/home/amdixit/24359628.img";
$output_file="/home/amdixit/temp/images/stream3/07354/24359628/ICO_59373346045_AWB_O_IAH_20071220_000000.tif";
$offset="392781";
$length="14585";
extract_tifs($copy_source,$output_file,$offset,$length);


