# ** ICOR Image Conversion Project **

This perl program was designed to convert ~70 million images out of a legacy storage platform.

Document batches are looked up by Julian date.  A batch could contain 1 to 40 images.  The database has a list of airbills (while relate directly to images) and the file offset (the image files are concatenated together, and the offset is where to pull the images (JPEGs) out.  After pulling out the file, the ImageMagick library is used to ensure the the resulting file is a valid .jpeg image.

The program takes the batch file and breaks it apart and creates independent .jpegs with a file name including airbill number, source and destination station, and a few custom identifiers.

