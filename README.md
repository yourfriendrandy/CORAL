# The Motif Attractor Algorithm

This project is presently compatible with Python 3.11.9, due to its reliance on Ray. The algorithm is hopefully cross-platform.

This repository contains an algorithm created to classify and analyze potential minimal convergent attractor sets in the 3n+1 (Collatz) problem. The tool was designed with the hope of easing analysis of the conjecture, and by no means attempts to formalize any deeper observations of phenomena. All observations of structural behavior to follow are given with the strict caveat that no counterexample has been found in the range tested (due to computational constraints, a range of 1 to 7 * 10^7 positive integers). 

The core algorithm identifies and manipulates a specific subset of odd and positive integers, A(n), that experience a one-step reduction in Collatz sequence length when multiplied by 3 prior to convergence. This reduction happens very early in the string for 3An, always decrementing by one odd step. These unique prefixes for A(n) and 3A(n) always converge at an even number L of the form 4 mod 6, and then share the same collapsing orbit back to 1. By generating these motifs and then attempting to 

This version includes:
Motif identification and classification for integer and product
Twin-only filtering (motifs where B = Y + 1), which are special cases to be delineated at a later date
Full convergence cycle storage
Dynamic reconstruction of all integers in range by use of a motif tree
Ray-based parallelization 

This is the earliest possible version of the project. Documentation, a plethora of ancillary analysis tools, and formal writeups will be added in future commits. The author lost an excruciatingly detailed README less than an hour ago due to the folly of direct web editing and pinky promises to explain everything more very soon. 

Thank you for your interest in this tool! The Collatz conjecture has spawned some wonderfully creative angles of approaching number theory. Hopefully this will be of some use to someone out there.

---

**License:** [CC BY-NC-SA 4.0](./LICENSE.txt)
