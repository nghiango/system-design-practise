package com.systemdesign.urlshortener.linkapi;

/** Deterministic Base62 encoder using the alphabet [a-zA-Z0-9]. */
final class Base62 {

  private static final String ALPHABET =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  private static final int BASE = ALPHABET.length();

  private Base62() {}

  static String encode(long value) {
    if (value < 0) {
      throw new IllegalArgumentException("value must be >= 0");
    }
    if (value == 0) {
      return String.valueOf(ALPHABET.charAt(0));
    }
    StringBuilder out = new StringBuilder();
    long remaining = value;
    while (remaining > 0) {
      out.append(ALPHABET.charAt((int) (remaining % BASE)));
      remaining /= BASE;
    }
    return out.reverse().toString();
  }
}
