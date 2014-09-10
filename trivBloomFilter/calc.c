#include <stdio.h>
#include <math.h>

int main() {
    double n = 100000;      // Estimated number of keys
    double p = 0.125;       // Allowed probability for false positives
    double m, k;

    m = -(((n) * log(p)) / (pow(log(2), 2)));

    printf("Size of bit array: %lf, approx: %.2lf\n", m, ceil(m));

    k = (m / n) * (log (2));

    printf("Number of hash functions: %lf, approx: %.2lf\n", k, ceil(k));

    return 0;
}
