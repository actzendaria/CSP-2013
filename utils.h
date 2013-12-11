#ifndef utils_h
#define utils_h

// The following func should be declared in another utils-like header.
//#define ERROR 0
//void print_bmap(char* buf);
inline void setn(int bit, unsigned char &byte)
{
  byte = (byte) | (0x1 << bit);
}

inline void unsetn(int bit, unsigned char &byte)
{
  byte = (byte) & (~(0x1 << bit));
}

inline unsigned char testn(int bit, unsigned char &byte)
{
  return (byte & ((0x00) | (0x1 << bit)));
}

// We use bit count 
inline int bit_count(unsigned char x)
{
  x = x - ((x >> 1) & 0x55);
  x = (x & 0x33) + ((x >> 2) & 0x33);
  x = (x + (x >> 4)) & 0x0F0F;

  return x & 0x0F;
}

#endif
