/* Generated By:JavaCC: Do not edit this line. parseTokenManager.java */
package drz.oddb.parse;
import java.util.*;
import drz.oddb.parseStmt.*;

/** Token Manager. */
public class parseTokenManager implements parseConstants
{

  /** Debug output. */
  public  java.io.PrintStream debugStream = System.out;
  /** Set debug output. */
  public  void setDebugStream(java.io.PrintStream ds) { debugStream = ds; }
private final int jjStopStringLiteralDfa_0(int pos, long active0)
{
   switch (pos)
   {
      case 0:
         if ((active0 & 0x8808000L) != 0L)
            return 10;
         if ((active0 & 0xd41f1fdcL) != 0L)
         {
            jjmatchedKind = 29;
            jjmatchedPos = 0;
            return 10;
         }
         if ((active0 & 0x2000L) != 0L)
            return 11;
         return -1;
      case 1:
         if ((active0 & 0x1400001cL) != 0L)
            return 10;
         if ((active0 & 0x801f1fc0L) != 0L)
         {
            jjmatchedKind = 29;
            jjmatchedPos = 1;
            return 10;
         }
         return -1;
      case 2:
         if ((active0 & 0x801f1fc0L) != 0L)
         {
            jjmatchedKind = 29;
            jjmatchedPos = 2;
            return 10;
         }
         return -1;
      case 3:
         if ((active0 & 0x20900L) != 0L)
            return 10;
         if ((active0 & 0x1d16c0L) != 0L)
         {
            jjmatchedKind = 29;
            jjmatchedPos = 3;
            return 10;
         }
         return -1;
      case 4:
         if ((active0 & 0x40200L) != 0L)
            return 10;
         if ((active0 & 0x1914c0L) != 0L)
         {
            jjmatchedKind = 29;
            jjmatchedPos = 4;
            return 10;
         }
         return -1;
      case 5:
         if ((active0 & 0x1914c0L) != 0L)
            return 10;
         return -1;
      case 6:
         if ((active0 & 0x100000L) != 0L)
         {
            jjmatchedKind = 29;
            jjmatchedPos = 6;
            return 10;
         }
         return -1;
      case 7:
         if ((active0 & 0x100000L) != 0L)
         {
            jjmatchedKind = 29;
            jjmatchedPos = 7;
            return 10;
         }
         return -1;
      case 8:
         if ((active0 & 0x100000L) != 0L)
         {
            jjmatchedKind = 29;
            jjmatchedPos = 8;
            return 10;
         }
         return -1;
      case 9:
         if ((active0 & 0x100000L) != 0L)
         {
            jjmatchedKind = 29;
            jjmatchedPos = 9;
            return 10;
         }
         return -1;
      case 10:
         if ((active0 & 0x100000L) != 0L)
         {
            jjmatchedKind = 29;
            jjmatchedPos = 10;
            return 10;
         }
         return -1;
      default :
         return -1;
   }
}
private final int jjStartNfa_0(int pos, long active0)
{
   return jjMoveNfa_0(jjStopStringLiteralDfa_0(pos, active0), pos + 1);
}
private int jjStopAtPos(int pos, int kind)
{
   jjmatchedKind = kind;
   jjmatchedPos = pos;
   return pos + 1;
}
private int jjMoveStringLiteralDfa0_0()
{
   switch(curChar)
   {
      case 40:
         return jjStartNfaWithStates_0(0, 13, 11);
      case 41:
         return jjStartNfaWithStates_0(0, 15, 10);
      case 43:
         return jjStartNfaWithStates_0(0, 29, 10);
      case 44:
         return jjStopAtPos(0, 14);
      case 45:
         return jjMoveStringLiteralDfa1_0(0x4000000L);
      case 46:
         return jjStartNfaWithStates_0(0, 27, 10);
      case 47:
         return jjMoveStringLiteralDfa1_0(0x1cL);
      case 59:
         return jjStopAtPos(0, 5);
      case 61:
         return jjStartNfaWithStates_0(0, 23, 10);
      case 65:
         return jjMoveStringLiteralDfa1_0(0x10000000L);
      case 67:
         return jjMoveStringLiteralDfa1_0(0x280L);
      case 68:
         return jjMoveStringLiteralDfa1_0(0x10100L);
      case 70:
         return jjMoveStringLiteralDfa1_0(0x20000L);
      case 73:
         return jjMoveStringLiteralDfa1_0(0xc00L);
      case 83:
         return jjMoveStringLiteralDfa1_0(0x80180000L);
      case 85:
         return jjMoveStringLiteralDfa1_0(0x40L);
      case 86:
         return jjMoveStringLiteralDfa1_0(0x1000L);
      case 87:
         return jjMoveStringLiteralDfa1_0(0x40000L);
      default :
         return jjMoveNfa_0(0, 0);
   }
}
private int jjMoveStringLiteralDfa1_0(long active0)
{
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(0, active0);
      return 1;
   }
   switch(curChar)
   {
      case 62:
         if ((active0 & 0x4000000L) != 0L)
            return jjStartNfaWithStates_0(1, 26, 10);
         break;
      case 65:
         return jjMoveStringLiteralDfa2_0(active0, 0x1000L);
      case 69:
         return jjMoveStringLiteralDfa2_0(active0, 0x80190000L);
      case 72:
         return jjMoveStringLiteralDfa2_0(active0, 0x40000L);
      case 76:
         return jjMoveStringLiteralDfa2_0(active0, 0x200L);
      case 78:
         return jjMoveStringLiteralDfa2_0(active0, 0xc00L);
      case 80:
         return jjMoveStringLiteralDfa2_0(active0, 0x40L);
      case 82:
         return jjMoveStringLiteralDfa2_0(active0, 0x20180L);
      case 83:
         if ((active0 & 0x10000000L) != 0L)
            return jjStartNfaWithStates_0(1, 28, 10);
         break;
      case 110:
         if ((active0 & 0x8L) != 0L)
            return jjStartNfaWithStates_0(1, 3, 10);
         break;
      case 114:
         if ((active0 & 0x10L) != 0L)
            return jjStartNfaWithStates_0(1, 4, 10);
         break;
      case 116:
         if ((active0 & 0x4L) != 0L)
            return jjStartNfaWithStates_0(1, 2, 10);
         break;
      default :
         break;
   }
   return jjStartNfa_0(0, active0);
}
private int jjMoveStringLiteralDfa2_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(0, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(1, active0);
      return 2;
   }
   switch(curChar)
   {
      case 65:
         return jjMoveStringLiteralDfa3_0(active0, 0x200L);
      case 68:
         return jjMoveStringLiteralDfa3_0(active0, 0x40L);
      case 69:
         return jjMoveStringLiteralDfa3_0(active0, 0x40080L);
      case 76:
         return jjMoveStringLiteralDfa3_0(active0, 0x191000L);
      case 79:
         return jjMoveStringLiteralDfa3_0(active0, 0x20100L);
      case 83:
         return jjMoveStringLiteralDfa3_0(active0, 0x400L);
      case 84:
         if ((active0 & 0x80000000L) != 0L)
            return jjStartNfaWithStates_0(2, 29, 10);
         return jjMoveStringLiteralDfa3_0(active0, 0x800L);
      default :
         break;
   }
   return jjStartNfa_0(1, active0);
}
private int jjMoveStringLiteralDfa3_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(1, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(2, active0);
      return 3;
   }
   switch(curChar)
   {
      case 65:
         return jjMoveStringLiteralDfa4_0(active0, 0xc0L);
      case 69:
         return jjMoveStringLiteralDfa4_0(active0, 0x190400L);
      case 77:
         if ((active0 & 0x20000L) != 0L)
            return jjStartNfaWithStates_0(3, 17, 10);
         break;
      case 79:
         if ((active0 & 0x800L) != 0L)
            return jjStartNfaWithStates_0(3, 11, 10);
         break;
      case 80:
         if ((active0 & 0x100L) != 0L)
            return jjStartNfaWithStates_0(3, 8, 10);
         break;
      case 82:
         return jjMoveStringLiteralDfa4_0(active0, 0x40000L);
      case 83:
         return jjMoveStringLiteralDfa4_0(active0, 0x200L);
      case 85:
         return jjMoveStringLiteralDfa4_0(active0, 0x1000L);
      default :
         break;
   }
   return jjStartNfa_0(2, active0);
}
private int jjMoveStringLiteralDfa4_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(2, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(3, active0);
      return 4;
   }
   switch(curChar)
   {
      case 67:
         return jjMoveStringLiteralDfa5_0(active0, 0x180000L);
      case 69:
         if ((active0 & 0x40000L) != 0L)
            return jjStartNfaWithStates_0(4, 18, 10);
         return jjMoveStringLiteralDfa5_0(active0, 0x1000L);
      case 82:
         return jjMoveStringLiteralDfa5_0(active0, 0x400L);
      case 83:
         if ((active0 & 0x200L) != 0L)
            return jjStartNfaWithStates_0(4, 9, 10);
         break;
      case 84:
         return jjMoveStringLiteralDfa5_0(active0, 0x100c0L);
      default :
         break;
   }
   return jjStartNfa_0(3, active0);
}
private int jjMoveStringLiteralDfa5_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(3, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(4, active0);
      return 5;
   }
   switch(curChar)
   {
      case 69:
         if ((active0 & 0x40L) != 0L)
            return jjStartNfaWithStates_0(5, 6, 10);
         else if ((active0 & 0x80L) != 0L)
            return jjStartNfaWithStates_0(5, 7, 10);
         else if ((active0 & 0x10000L) != 0L)
            return jjStartNfaWithStates_0(5, 16, 10);
         break;
      case 83:
         if ((active0 & 0x1000L) != 0L)
            return jjStartNfaWithStates_0(5, 12, 10);
         break;
      case 84:
         if ((active0 & 0x400L) != 0L)
            return jjStartNfaWithStates_0(5, 10, 10);
         else if ((active0 & 0x80000L) != 0L)
         {
            jjmatchedKind = 19;
            jjmatchedPos = 5;
         }
         return jjMoveStringLiteralDfa6_0(active0, 0x100000L);
      default :
         break;
   }
   return jjStartNfa_0(4, active0);
}
private int jjMoveStringLiteralDfa6_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(4, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(5, active0);
      return 6;
   }
   switch(curChar)
   {
      case 68:
         return jjMoveStringLiteralDfa7_0(active0, 0x100000L);
      default :
         break;
   }
   return jjStartNfa_0(5, active0);
}
private int jjMoveStringLiteralDfa7_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(5, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(6, active0);
      return 7;
   }
   switch(curChar)
   {
      case 69:
         return jjMoveStringLiteralDfa8_0(active0, 0x100000L);
      default :
         break;
   }
   return jjStartNfa_0(6, active0);
}
private int jjMoveStringLiteralDfa8_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(6, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(7, active0);
      return 8;
   }
   switch(curChar)
   {
      case 80:
         return jjMoveStringLiteralDfa9_0(active0, 0x100000L);
      default :
         break;
   }
   return jjStartNfa_0(7, active0);
}
private int jjMoveStringLiteralDfa9_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(7, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(8, active0);
      return 9;
   }
   switch(curChar)
   {
      case 85:
         return jjMoveStringLiteralDfa10_0(active0, 0x100000L);
      default :
         break;
   }
   return jjStartNfa_0(8, active0);
}
private int jjMoveStringLiteralDfa10_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(8, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(9, active0);
      return 10;
   }
   switch(curChar)
   {
      case 84:
         return jjMoveStringLiteralDfa11_0(active0, 0x100000L);
      default :
         break;
   }
   return jjStartNfa_0(9, active0);
}
private int jjMoveStringLiteralDfa11_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(9, old0);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(10, active0);
      return 11;
   }
   switch(curChar)
   {
      case 89:
         if ((active0 & 0x100000L) != 0L)
            return jjStartNfaWithStates_0(11, 20, 10);
         break;
      default :
         break;
   }
   return jjStartNfa_0(10, active0);
}
private int jjStartNfaWithStates_0(int pos, int kind, int state)
{
   jjmatchedKind = kind;
   jjmatchedPos = pos;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) { return pos + 1; }
   return jjMoveNfa_0(state, pos + 1);
}
private int jjMoveNfa_0(int startState, int curPos)
{
   int startsAt = 0;
   jjnewStateCnt = 11;
   int i = 1;
   jjstateSet[0] = startState;
   int kind = 0x7fffffff;
   for (;;)
   {
      if (++jjround == 0x7fffffff)
         ReInitRounds();
      if (curChar < 64)
      {
         long l = 1L << curChar;
         do
         {
            switch(jjstateSet[--i])
            {
               case 0:
                  if ((0x73ffef0600000000L & l) != 0L)
                  {
                     if (kind > 29)
                        kind = 29;
                     jjCheckNAdd(10);
                  }
                  if ((0x3fe000000000000L & l) != 0L)
                  {
                     if (kind > 24)
                        kind = 24;
                     jjCheckNAdd(6);
                  }
                  else if (curChar == 34)
                     jjCheckNAddTwoStates(8, 9);
                  else if (curChar == 48)
                  {
                     if (kind > 24)
                        kind = 24;
                  }
                  else if (curChar == 40)
                  {
                     if (kind > 22)
                        kind = 22;
                     jjCheckNAdd(3);
                  }
                  break;
               case 11:
                  if ((0x73ffef0600000000L & l) != 0L)
                  {
                     if (kind > 29)
                        kind = 29;
                     jjCheckNAdd(10);
                  }
                  if ((0x3ffaf0000000000L & l) != 0L)
                  {
                     if (kind > 22)
                        kind = 22;
                     jjCheckNAdd(3);
                  }
                  break;
               case 1:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 21)
                     kind = 21;
                  jjstateSet[jjnewStateCnt++] = 1;
                  break;
               case 2:
                  if (curChar != 40)
                     break;
                  if (kind > 22)
                     kind = 22;
                  jjCheckNAdd(3);
                  break;
               case 3:
                  if ((0x3ffaf0000000000L & l) == 0L)
                     break;
                  if (kind > 22)
                     kind = 22;
                  jjCheckNAdd(3);
                  break;
               case 4:
                  if (curChar == 48 && kind > 24)
                     kind = 24;
                  break;
               case 5:
                  if ((0x3fe000000000000L & l) == 0L)
                     break;
                  if (kind > 24)
                     kind = 24;
                  jjCheckNAdd(6);
                  break;
               case 6:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 24)
                     kind = 24;
                  jjCheckNAdd(6);
                  break;
               case 7:
                  if (curChar == 34)
                     jjCheckNAddTwoStates(8, 9);
                  break;
               case 8:
                  if ((0x3fe000000000000L & l) != 0L)
                     jjCheckNAddTwoStates(8, 9);
                  break;
               case 9:
                  if (curChar == 34 && kind > 25)
                     kind = 25;
                  break;
               case 10:
                  if ((0x73ffef0600000000L & l) == 0L)
                     break;
                  if (kind > 29)
                     kind = 29;
                  jjCheckNAdd(10);
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else if (curChar < 128)
      {
         long l = 1L << (curChar & 077);
         do
         {
            switch(jjstateSet[--i])
            {
               case 0:
                  if ((0x7fffffe07fffffeL & l) != 0L)
                  {
                     if (kind > 29)
                        kind = 29;
                     jjCheckNAdd(10);
                  }
                  else if ((0x800000008000000L & l) != 0L)
                  {
                     if (kind > 22)
                        kind = 22;
                     jjCheckNAdd(3);
                  }
                  if ((0x7fffffe00000000L & l) != 0L)
                  {
                     if (kind > 21)
                        kind = 21;
                     jjCheckNAdd(1);
                  }
                  break;
               case 11:
                  if ((0x2ffffffe2ffffffeL & l) != 0L)
                  {
                     if (kind > 22)
                        kind = 22;
                     jjCheckNAdd(3);
                  }
                  if ((0x7fffffe07fffffeL & l) != 0L)
                  {
                     if (kind > 29)
                        kind = 29;
                     jjCheckNAdd(10);
                  }
                  break;
               case 1:
                  if ((0x7fffffe07fffffeL & l) == 0L)
                     break;
                  if (kind > 21)
                     kind = 21;
                  jjCheckNAdd(1);
                  break;
               case 2:
                  if ((0x800000008000000L & l) == 0L)
                     break;
                  if (kind > 22)
                     kind = 22;
                  jjCheckNAdd(3);
                  break;
               case 3:
                  if ((0x2ffffffe2ffffffeL & l) == 0L)
                     break;
                  if (kind > 22)
                     kind = 22;
                  jjCheckNAdd(3);
                  break;
               case 8:
                  if ((0x7fffffe07fffffeL & l) != 0L)
                     jjAddStates(0, 1);
                  break;
               case 10:
                  if ((0x7fffffe07fffffeL & l) == 0L)
                     break;
                  if (kind > 29)
                     kind = 29;
                  jjCheckNAdd(10);
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else
      {
         int i2 = (curChar & 0xff) >> 6;
         long l2 = 1L << (curChar & 077);
         do
         {
            switch(jjstateSet[--i])
            {
               default : break;
            }
         } while(i != startsAt);
      }
      if (kind != 0x7fffffff)
      {
         jjmatchedKind = kind;
         jjmatchedPos = curPos;
         kind = 0x7fffffff;
      }
      ++curPos;
      if ((i = jjnewStateCnt) == (startsAt = 11 - (jjnewStateCnt = startsAt)))
         return curPos;
      try { curChar = input_stream.readChar(); }
      catch(java.io.IOException e) { return curPos; }
   }
}
static final int[] jjnextStates = {
   8, 9, 
};

/** Token literal values. */
public static final String[] jjstrLiteralImages = {
"", null, null, null, null, "\73", "\125\120\104\101\124\105", 
"\103\122\105\101\124\105", "\104\122\117\120", "\103\114\101\123\123", "\111\116\123\105\122\124", 
"\111\116\124\117", "\126\101\114\125\105\123", "\50", "\54", "\51", "\104\105\114\105\124\105", 
"\106\122\117\115", "\127\110\105\122\105", "\123\105\114\105\103\124", 
"\123\105\114\105\103\124\104\105\120\125\124\131", null, null, "\75", null, null, "\55\76", "\56", "\101\123", null, "\53", 
"\123\105\124", };

/** Lexer state names. */
public static final String[] lexStateNames = {
   "DEFAULT",
};
static final long[] jjtoToken = {
   0xffffffe1L, 
};
static final long[] jjtoSkip = {
   0x1eL, 
};
protected SimpleCharStream input_stream;
private final int[] jjrounds = new int[11];
private final int[] jjstateSet = new int[22];
private final StringBuilder jjimage = new StringBuilder();
private StringBuilder image = jjimage;
private int jjimageLen;
private int lengthOfMatch;
protected char curChar;
/** Constructor. */
public parseTokenManager(SimpleCharStream stream){
   if (SimpleCharStream.staticFlag)
      throw new Error("ERROR: Cannot use a static CharStream class with a non-static lexical analyzer.");
   input_stream = stream;
}

/** Constructor. */
public parseTokenManager(SimpleCharStream stream, int lexState){
   this(stream);
   SwitchTo(lexState);
}

/** Reinitialise parser. */
public void ReInit(SimpleCharStream stream)
{
   jjmatchedPos = jjnewStateCnt = 0;
   curLexState = defaultLexState;
   input_stream = stream;
   ReInitRounds();
}
private void ReInitRounds()
{
   int i;
   jjround = 0x80000001;
   for (i = 11; i-- > 0;)
      jjrounds[i] = 0x80000000;
}

/** Reinitialise parser. */
public void ReInit(SimpleCharStream stream, int lexState)
{
   ReInit(stream);
   SwitchTo(lexState);
}

/** Switch to specified lex state. */
public void SwitchTo(int lexState)
{
   if (lexState >= 1 || lexState < 0)
      throw new TokenMgrError("Error: Ignoring invalid lexical state : " + lexState + ". State unchanged.", TokenMgrError.INVALID_LEXICAL_STATE);
   else
      curLexState = lexState;
}

protected Token jjFillToken()
{
   final Token t;
   final String curTokenImage;
   final int beginLine;
   final int endLine;
   final int beginColumn;
   final int endColumn;
   if (jjmatchedPos < 0)
   {
      if (image == null)
         curTokenImage = "";
      else
         curTokenImage = image.toString();
      beginLine = endLine = input_stream.getBeginLine();
      beginColumn = endColumn = input_stream.getBeginColumn();
   }
   else
   {
      String im = jjstrLiteralImages[jjmatchedKind];
      curTokenImage = (im == null) ? input_stream.GetImage() : im;
      beginLine = input_stream.getBeginLine();
      beginColumn = input_stream.getBeginColumn();
      endLine = input_stream.getEndLine();
      endColumn = input_stream.getEndColumn();
   }
   t = Token.newToken(jjmatchedKind, curTokenImage);

   t.beginLine = beginLine;
   t.endLine = endLine;
   t.beginColumn = beginColumn;
   t.endColumn = endColumn;

   return t;
}

int curLexState = 0;
int defaultLexState = 0;
int jjnewStateCnt;
int jjround;
int jjmatchedPos;
int jjmatchedKind;

/** Get the next Token. */
public Token getNextToken() 
{
  Token matchedToken;
  int curPos = 0;

  EOFLoop :
  for (;;)
  {
   try
   {
      curChar = input_stream.BeginToken();
   }
   catch(java.io.IOException e)
   {
      jjmatchedKind = 0;
      matchedToken = jjFillToken();
      return matchedToken;
   }
   image = jjimage;
   image.setLength(0);
   jjimageLen = 0;

   try { input_stream.backup(0);
      while (curChar <= 32 && (0x100000000L & (1L << curChar)) != 0L)
         curChar = input_stream.BeginToken();
   }
   catch (java.io.IOException e1) { continue EOFLoop; }
   jjmatchedKind = 29;
   jjmatchedPos = -1;
   curPos = 0;
   curPos = jjMoveStringLiteralDfa0_0();
   if (jjmatchedKind != 0x7fffffff)
   {
      if (jjmatchedPos + 1 < curPos)
         input_stream.backup(curPos - jjmatchedPos - 1);
      if ((jjtoToken[jjmatchedKind >> 6] & (1L << (jjmatchedKind & 077))) != 0L)
      {
         matchedToken = jjFillToken();
         TokenLexicalActions(matchedToken);
         return matchedToken;
      }
      else
      {
         continue EOFLoop;
      }
   }
   int error_line = input_stream.getEndLine();
   int error_column = input_stream.getEndColumn();
   String error_after = null;
   boolean EOFSeen = false;
   try { input_stream.readChar(); input_stream.backup(1); }
   catch (java.io.IOException e1) {
      EOFSeen = true;
      error_after = curPos <= 1 ? "" : input_stream.GetImage();
      if (curChar == '\n' || curChar == '\r') {
         error_line++;
         error_column = 0;
      }
      else
         error_column++;
   }
   if (!EOFSeen) {
      input_stream.backup(1);
      error_after = curPos <= 1 ? "" : input_stream.GetImage();
   }
   throw new TokenMgrError(EOFSeen, curLexState, error_line, error_column, error_after, curChar, TokenMgrError.LEXICAL_ERROR);
  }
}

int[] jjemptyLineNo = new int[1];
int[] jjemptyColNo = new int[1];
boolean[] jjbeenHere = new boolean[1];
void TokenLexicalActions(Token matchedToken)
{
   switch(jjmatchedKind)
   {
      case 0 :
         break;
      case 5 :
         break;
      case 6 :
         break;
      case 7 :
         break;
      case 8 :
         break;
      case 9 :
         break;
      case 10 :
         break;
      case 11 :
         break;
      case 12 :
         break;
      case 13 :
         break;
      case 14 :
         break;
      case 15 :
         break;
      case 16 :
         break;
      case 17 :
         break;
      case 18 :
         break;
      case 19 :
         break;
      case 20 :
         break;
      case 21 :
         break;
      case 22 :
         break;
      case 23 :
         break;
      case 24 :
         break;
      case 25 :
         break;
      case 26 :
         break;
      case 27 :
         break;
      case 28 :
         break;
      case 29 :
         if (jjmatchedPos == -1)
         {
            if (jjbeenHere[0] &&
                jjemptyLineNo[0] == input_stream.getBeginLine() &&
                jjemptyColNo[0] == input_stream.getBeginColumn())
               throw new TokenMgrError(("Error: Bailing out of infinite loop caused by repeated empty string matches at line " + input_stream.getBeginLine() + ", column " + input_stream.getBeginColumn() + "."), TokenMgrError.LOOP_DETECTED);
            jjemptyLineNo[0] = input_stream.getBeginLine();
            jjemptyColNo[0] = input_stream.getBeginColumn();
            jjbeenHere[0] = true;
         }
         break;
      case 30 :
         break;
      case 31 :
         break;
      default :
         break;
   }
}
private void jjCheckNAdd(int state)
{
   if (jjrounds[state] != jjround)
   {
      jjstateSet[jjnewStateCnt++] = state;
      jjrounds[state] = jjround;
   }
}
private void jjAddStates(int start, int end)
{
   do {
      jjstateSet[jjnewStateCnt++] = jjnextStates[start];
   } while (start++ != end);
}
private void jjCheckNAddTwoStates(int state1, int state2)
{
   jjCheckNAdd(state1);
   jjCheckNAdd(state2);
}

}