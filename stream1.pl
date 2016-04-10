#!/usr/bin/perl -w

use strict;
use lib qw(/home/amdixit/conversion/lib);
use lib "/opt/oracle/scripts/perl/new/lib64/perl5/site_perl/5.8.5/x86_64-linux-thread-multi";
# local modules (lib)
use date_functions;
use file_functions;
use logger;
use DB;

# cpan modules
use File::Basename;
use Data::Dumper;
use DBI;

#########################
# CONFIGURABLE ITEMS    #
# Per stream settings   #
# Stream 1:  06001-06182#
#########################
# Note: source Oracle environment variables before running script,
# unless already set 
########################
my $stream_id = "1";
my $storage_path = "/home/amdixit/conversion/tmp/1/";
my $file_path = "/images/stream1/";

# database variables
my $dbname = "icoprod";
my $dbuser = "icor";
my $dbpass = "icorprod";

# return array of julian days we are going to process with this stream
my @dates = build_array("06","001","365");

#############################
# END OF CONFIGURABLE ITEMS #
#############################

# make sure everything exists
if ( ! -d $storage_path ) {
	die "Missing $storage_path.  Stopping.";
}

if ( ! -d $file_path ) {
	die "Missing $file_path.  Stopping.";
}

# open cached database handle
# each subroutine below will attempt to use this handle,
# and should prevent opening more than one at a time per stream
my $dbh = DBI->connect_cached("dbi:Oracle:$dbname",$dbuser,$dbpass) or die "Couldn't connect to database: $dbname";
write_log($stream_id,"##### Startup stream: $stream_id #####","1");
write_log($stream_id,"Database connected","1");

# Begin working on the request Julian dates
# Julian Day Loop
foreach my $date (@dates) {
	write_log($stream_id,"Starting on $date","1");
	my $julian_path = "$file_path$date";
	if ( check_complete_flag($julian_path) ) {
		write_log($stream_id,"$julian_path is marked COMPLETE","2");
		next;
	}
	if ( ! create_directory($julian_path) ) {
		write_error($stream_id,"Error creating directory: $julian_path");
		die "Error creating directory: $julian_path";
	}
	write_log($stream_id,"created directory $julian_path","3");
	my $short_date = julian_to_short($date);
	write_log($stream_id,"short date is set to $short_date","3");

	# pull back the batches from the database
	my @batches;
	my $batch_sql = get_batch_list_sql();
	$dbh = DBI->connect_cached("dbi:Oracle:$dbname",$dbuser,$dbpass) or write_error($stream_id,"Couldn't connect to DB.") && die "Couldn't connect to database: $dbname";
	my $batch_query = $dbh->prepare($batch_sql);
	$batch_query->execute($short_date);
		while (my $row = $batch_query->fetchrow_array) {
			push ( @batches, $row );
		}
	my $batch_length = @batches;
	write_log($stream_id,"returned $batch_length batches","2");
		# Batch Loop
		foreach my $batch_num (@batches) {
			my $batch_path = "$file_path$date/$batch_num";
			write_log($stream_id,"starting batch number $batch_num","3");
			if ( check_complete_flag($batch_path) ) {
				write_log($stream_id,"$batch_path is marked COMPLETE","2");
				next;
			}
			if ( ! create_directory($batch_path) ) {
				write_error($stream_id,"Error creating directory: $batch_path");
				die "Error creating directory: $batch_path";
			}
			# directory is set up, get database details for batch
			my $this_batch = get_batch_details($batch_num);

			#unpack the hash data
			my $image_file_nam = $this_batch->{'IMAGE_FILE_NAM'};
                        if ($image_file_nam) { #praveen
  
			    my $file_basename = basename($image_file_nam);
			    my $arbl_ct = $this_batch->{'ARBL_CT'};
			    my $create_date = $this_batch->{'CREAT_DT'};
			    my $origin_id = $this_batch->{'ORGN_TRF_PT_ID'};
			    write_log($stream_id,"details for $batch_num: $image_file_nam $arbl_ct $create_date $origin_id","2");
			    # copy the img file local
			    my $copy_dest = $storage_path;
			    my $copy_source = "$storage_path$file_basename";
			    if ( get_file($stream_id,$image_file_nam,$copy_dest) ) {
			        	write_error($stream_id,"error copying $image_file_nam to $copy_dest");
				        die "error copying $image_file_nam to $copy_dest";
			    }
			    write_log($stream_id,"wrote $image_file_nam","2");
			    # we have a good img file copied locally, start extracting the tifs 
			
			    my @these_airbills = get_airbill_list($batch_num);
			    my %airbill_details;
			    # Airbill Loop
			    foreach my $this_airbill (@these_airbills) {
			        	%airbill_details = get_airbill_details($this_airbill,$batch_num);
				        my $airbill = $airbill_details{'ARBL_NBR'};
				        my $offset = $airbill_details{'IMAGE_FILE_OFSET'};
				        my $length = $airbill_details{'IMAGE_LEN'};
				        write_log($stream_id,"starting awb: $airbill offset: $offset length: $length","3");
				        # determine final airbill filename
				        my $output_tif_name = create_file_name($airbill,$create_date,$origin_id);
				        unless ($output_tif_name) {
				 	       write_error($stream_id,"Could not create filename for airbill: $airbill");
					       die "Could not create filename for airbill: $airbill";
				        }
				        my $output_file = "$file_path$date/$batch_num/$output_tif_name";
				        # do the cut for this airbill
				        unless ( extract_tifs($copy_source,$output_file,$offset,$length) ) {
					       write_error($stream_id,"failed to create tif from: $copy_source $output_file");
				  	       die "failed to create tif from: $copy_source to $output_file";
				        }
				        # make sure created tif is valid.  read tif headers
				        unless ( check_tif($output_file) ) {
					       write_error($stream_id,"failed checking created tif: $output_file");
					       die "failed checking created tif: $output_file";
				        }
				        write_log($stream_id,"created: $output_file","3");
			                # Airbill Loop is complete
			    }
		            # remove local copy of .img file
		            unless ( rm_temp_file($copy_source) ) {
			           write_error($stream_id,"could not remove temp file: $copy_source");
			           die "could not remove temp file: $copy_source";
		            }
                    
                        } #praveen
                        else #praveen
                        { 
                           write_log($stream_id,"Batch: null batch found for $batch_num","2"); #praveen

                        }  #praveen

		my $output_dir = "$file_path$date/$batch_num/";
		# mark this batch as complete
		unless ( write_complete_flag($output_dir) ) {
			write_error($stream_id,"could not set $output_dir as .complete");
			die "could not set $output_dir as .complete";
		}
		write_log($stream_id,"Batch: $batch_num complete","2");
		# Batch Loop is complete
		}
# mark this day as complete
unless ( write_complete_flag($julian_path . "/") ) {
	write_error($stream_id,"Could not set day $julian_path as .complete");
	die "Could not set day $julian_path as .complete";
}
write_log($stream_id,"Day: $date is complete","1");
# Julian Day Loop  is complete
}
write_log($stream_id,"##### Reached normal end of program for stream: $stream_id #####","1");
$dbh->disconnect or die "Could not disconnect from database";

#### Subroutines ####

sub get_batch_details {
	my $current_batch = shift;
	my %batch_info;
	my $batch_detail_sql = get_batch_details_sql();
	$dbh = DBI->connect_cached("dbi:Oracle:$dbname",$dbuser,$dbpass) or write_error($stream_id,"Couldn't connect to DB: $dbname") && die "Couldn't connect to database: $dbname";
	my $batch_detail_query = $dbh->prepare($batch_detail_sql);
	$batch_detail_query->execute($current_batch);
	while ( my $item = $batch_detail_query->fetchrow_hashref ) {
		for my $key ( sort keys %$item ) {
			my $this_value = $item->{ $key };
			$batch_info{$key} = $this_value;
		}
	}
	return \%batch_info;
}

sub get_airbill_list {
	my $current_batch = shift;
	my @airbills;
	my $airbill_list_sql = get_airbill_list_sql();
	$dbh = DBI->connect_cached("dbi:Oracle:$dbname",$dbuser,$dbpass) or write_error($stream_id,"Couldn't connect to DB: $dbname") && die "Couldn't connect to database: $dbname";
	my $airbill_list_query = $dbh->prepare($airbill_list_sql);
	$airbill_list_query->execute($current_batch);
	while ( my $item = $airbill_list_query->fetchrow_array ) {
		push(@airbills,$item);
	}	
	return @airbills;
}

sub get_airbill_details {
	my $current_airbill = shift;
	my $batch_num = shift;
	my %airbill_detail;
	my $airbill_detail_sql = get_airbill_details_sql();
	$dbh = DBI->connect_cached("dbi:Oracle:$dbname",$dbuser,$dbpass) or write_error($stream_id,"Couldn't connect to DB: $dbname") && die "Couldn't connect to database: $dbname";
	my $airbill_detail_query = $dbh->prepare($airbill_detail_sql);
	$airbill_detail_query->execute($current_airbill,$batch_num);
	while ( my $item = $airbill_detail_query->fetchrow_hashref ) {
		for my $key ( sort keys %$item ) {
			my $this_value = $item->{ $key };
			$airbill_detail{$key} = $this_value;
		}
	}	
	$airbill_detail{ARBL_NBR} = $current_airbill;
	return %airbill_detail; 
}

