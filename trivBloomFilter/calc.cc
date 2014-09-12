#include <stdio.h>
#include <math.h>

#include <sstream>

int main(int argc, char *argv[]) {

    if (argc != 3) {
        printf("Usage: make calculate <key_count> <false_postive_probability>\n");
        return 0;
    }

    std::stringstream ss1(argv[1]);
    std::stringstream ss2(argv[2]);
    double n;       // Estimated number of keys
    double p;       // Allowed probability for false positives
    double m, k;

    if (!(ss1 >> n) || !(ss2 >> p)) {
        printf("Error: Invalid numbers\n");
        printf("Usage: make calculate <key_count> <false_postive_probability>\n");
        return 0;
    }

    m = -(((n) * log(p)) / (pow(log(2), 2)));

    printf("Size of bit array: %lf, approx: %.2lf\n", m, round(m));

    k = (m / n) * (log (2));

    printf("Number of hash functions: %lf, approx: %.2lf\n", k, round(k));

    return 0;
}
