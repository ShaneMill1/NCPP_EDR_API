/* gd/config.h.  Generated from config.hin by configure.  */
/* config.hin.  Generated from configure.ac by autoheader.  */

/* Define to 1 if you have the <dlfcn.h> header file. */
/* #undef HAVE_DLFCN_H */

/* Define to 1 if you have the <errno.h> header file. */
/* #undef HAVE_ERRNO_H */

/* Define to 1 if you have the <ft2build.h> header file. */
/* #undef HAVE_FT2BUILD_H */

/* Define if you have the iconv() function. */
/* #undef HAVE_ICONV */

/* Define to 1 if you have the <iconv.h> header file. */
/* #undef HAVE_ICONV_H */

/* Define if <iconv.h> defines iconv_t. */
/* #undef HAVE_ICONV_T_DEF */

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define if you have the fontconfig library. */
/* #undef HAVE_LIBFONTCONFIG */

/* Define if you have the freetype library. */
/* #undef HAVE_LIBFREETYPE */

/* Define if you have the jpeg library. */
/* #undef HAVE_LIBJPEG */

/* Define to 1 if you have the `m' library (-lm). */
/* #undef HAVE_LIBM */

/* Define if you have the png library. */
/* #undef HAVE_LIBPNG */

/* Define to 1 if you have the <libpng/png.h> header file. */
/* #undef HAVE_LIBPNG_PNG_H */

/* Define if you have the Xpm library. */
/* #undef HAVE_LIBXPM */

/* Define if you have zlib. */
/* #undef HAVE_LIBZ */

/* Define to 1 if you have the <limits.h> header file. */
/* #undef HAVE_LIMITS_H */

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the <png.h> header file. */
/* #undef HAVE_PNG_H */

/* Define if you have POSIX threads libraries and header files. */
/* #undef HAVE_PTHREAD */

/* Define to 1 if you have the <stddef.h> header file. */
/* #undef HAVE_STDDEF_H */

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define as const if the declaration of iconv() needs const. */
/* #undef ICONV_CONST */

/* Name of package */
/* #undef PACKAGE */

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "arthur.taylor@noaa.gov"

/* Define to the full name of this package. */
#define PACKAGE_NAME "degrib"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "degrib 2.15"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "degrib"

/* Define to the version of this package. */
#define PACKAGE_VERSION "2.15"

/* Define to the necessary symbol if this constant uses a non-standard name on
   your system. */
/* #undef PTHREAD_CREATE_JOINABLE */

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* Version number of package */
/* #undef VERSION */

/*Added 9/21/2006 by ADTaylor to ensure R_OK defined even if unistd.h broken */
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#ifndef R_OK
#define R_OK 04
#endif
#endif
