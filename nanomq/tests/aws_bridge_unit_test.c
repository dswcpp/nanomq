#include <assert.h>

#if defined(SUPP_AWS_BRIDGE)

#include "../aws_bridge.c"

int
main(void)
{
    return 0;
}

#else

int
main(void)
{
    /* Current default local build does not enable AWS bridge support. */
    return 0;
}

#endif
