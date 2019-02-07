package apophenia

import "fmt"

// Uint128 is an array of 2 uint64, treated as a single
// object to simplify calling conventions.
type Uint128 struct {
	Lo, Hi uint64 // low-order and high-order uint64 words. Value is ``(Hi << 64) | Lo`.
}

// Add adds value to its receiver in place.
func (u *Uint128) Add(value Uint128) {
	u.Lo += value.Lo
	if u.Lo < value.Lo {
		u.Hi++
	}
	u.Hi += value.Hi
}

// Sub subtracts value from its receiver in place.
func (u *Uint128) Sub(value Uint128) {
	u.Lo -= value.Lo
	if u.Lo > value.Lo {
		u.Hi--
	}
	u.Hi -= value.Hi
}

// And does a bitwise and with value, in place.
func (u *Uint128) And(value Uint128) {
	u.Lo, u.Hi = u.Lo&value.Lo, u.Hi&value.Hi
}

// Or does a bitwise or with value, in place.
func (u *Uint128) Or(value Uint128) {
	u.Lo, u.Hi = u.Lo|value.Lo, u.Hi|value.Hi
}

// Xor does a bitwise xor with value, in place.
func (u *Uint128) Xor(value Uint128) {
	u.Lo, u.Hi = u.Lo^value.Lo, u.Hi^value.Hi
}

// Not does a bitwise complement in place.
func (u *Uint128) Not() {
	u.Lo, u.Hi = ^u.Lo, ^u.Hi
}

// Mask produces a mask of the lower n bits of u.
func (u *Uint128) Mask(n uint64) {
	if n >= 128 {
		return
	}
	if n >= 64 {
		u.Hi &= (1 << (n & 63)) - 1
	} else {
		u.Lo &= (1 << n) - 1
	}
}

// Mask produces a bitmask with n bits set.
func Mask(n uint64) (u Uint128) {
	if n >= 128 {
		u.Not()
		return u
	}
	if n >= 64 {
		u.Lo--
		u.Hi = (1 << n & 63) - 1
		return u
	}
	u.Lo = (1 << n) - 1
	return u
}

// String provides a string representation.
func (u Uint128) String() string {
	return fmt.Sprintf("0x%x%016x", u.Hi, u.Lo)
}

// bit rotation: for 1-63 bits, we are moving the low-order N bits of u.Lo
// into the high-order N bits of u.Hi, and vice versa. For 64-127, it's that
// plus swapping u.Lo and u.Hi.

// RotateRight rotates u right by n bits.
func (u *Uint128) RotateRight(n uint64) {
	if n&64 != 0 {
		u.Lo, u.Hi = u.Hi, u.Lo
	}
	n &= 63
	if n == 0 {
		return
	}
	unbits := 64 - n

	u.Lo, u.Hi = (u.Lo>>n)|(u.Hi<<unbits), (u.Hi>>n)|(u.Lo<<unbits)
}

// RotateLeft rotates u left by n bits.
func (u *Uint128) RotateLeft(n uint64) {
	if n&64 != 0 {
		u.Lo, u.Hi = u.Hi, u.Lo
	}
	n &= 63
	if n == 0 {
		return
	}
	unbits := 64 - n

	u.Lo, u.Hi = (u.Lo<<n)|(u.Hi>>unbits), (u.Hi<<n)|(u.Lo>>unbits)
}

// ShiftRight shifts u right by n bits.
func (u *Uint128) ShiftRight(n uint64) {
	if n > 127 {
		u.Lo, u.Hi = 0, 0
		return
	}
	if n >= 64 {
		u.Lo, u.Hi = u.Hi>>(n&63), 0
		return
	}
	unbits := 64 - n

	u.Lo, u.Hi = (u.Lo>>n)|(u.Hi<<unbits), (u.Hi >> n)
}

// ShiftRightCarry returns both the shifted value and the bits that
// were shifted out. Useful for when you want both x%N and x/N for
// N a power of 2. Only sane if bits <= 64.
func (u *Uint128) ShiftRightCarry(n uint64) (out Uint128, carry uint64) {
	if n > 64 {
		return out, carry
	}
	if n == 64 {
		out.Lo, carry = u.Hi, u.Lo
		return out, carry
	}
	unbits := 64 - n

	out.Lo, out.Hi, carry = (u.Lo>>n)|(u.Hi<<unbits), (u.Hi >> n), u.Lo&((1<<n)-1)
	return out, carry
}

// ShiftLeft rotates u left by n bits.
func (u *Uint128) ShiftLeft(n uint64) {
	if n > 127 {
		u.Lo, u.Hi = 0, 0
		return
	}
	if n >= 64 {
		u.Lo, u.Hi = 0, u.Lo<<(n&63)
		return
	}
	n &= 63
	if n == 0 {
		return
	}
	unbits := 64 - n

	u.Lo, u.Hi = (u.Lo << n), (u.Hi<<n)|(u.Lo>>unbits)
}

// Bit returns 1 if the nth bit is set, 0 otherwise.
func (u *Uint128) Bit(n uint64) uint64 {
	if n >= 128 {
		return 0
	}
	if n >= 64 {
		return (u.Hi >> (n & 63)) & 1
	}
	return (u.Lo >> n) & 1
}

// Inc increments its receiver in place.
func (u *Uint128) Inc() {
	u.Lo++
	if u.Lo == 0 {
		u.Hi++
	}
}
