package com.systemdesign.urlshortener.linkapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class Base62Test {

  @Test
  void encodesZero() {
    assertThat(Base62.encode(0)).isEqualTo("a");
  }

  @Test
  void encodesWithinAlphabet() {
    assertThat(Base62.encode(1)).isEqualTo("b");
    assertThat(Base62.encode(25)).isEqualTo("z");
    assertThat(Base62.encode(26)).isEqualTo("A");
    assertThat(Base62.encode(51)).isEqualTo("Z");
    assertThat(Base62.encode(52)).isEqualTo("0");
    assertThat(Base62.encode(61)).isEqualTo("9");
  }

  @Test
  void encodesMultipleDigits() {
    // 62 = 1*62 + 0 -> "ba" with the [a-zA-Z0-9] alphabet.
    assertThat(Base62.encode(62)).isEqualTo("ba");
    assertThat(Base62.encode(63)).isEqualTo("bb");
  }

  @Test
  void isDeterministicAndOnlyUsesAlphabet() {
    for (long id : new long[] {1, 62, 3844, 238_328, 1_000_000_000L, Long.MAX_VALUE}) {
      String encoded = Base62.encode(id);
      assertThat(encoded).matches("[a-zA-Z0-9]+");
      assertThat(Base62.encode(id)).isEqualTo(encoded);
    }
  }

  @Test
  void rejectsNegativeValues() {
    assertThatThrownBy(() -> Base62.encode(-1)).isInstanceOf(IllegalArgumentException.class);
  }
}
