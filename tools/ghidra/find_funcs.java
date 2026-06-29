import ghidra.app.script.GhidraScript;
import ghidra.app.decompiler.*;
import ghidra.program.model.listing.*;
import ghidra.program.model.address.*;
import ghidra.program.model.mem.*;
import ghidra.util.task.ConsoleTaskMonitor;
import java.util.*;

public class find_funcs extends GhidraScript {
  public void run() throws Exception {
    String[] names = {"get_signature","sign","send_extend_handshake","make_mi","got_extend_handshake"};
    Memory mem = currentProgram.getMemory();
    FunctionManager fm = currentProgram.getFunctionManager();
    DecompInterface dec = new DecompInterface(); dec.openProgram(currentProgram);
    ConsoleTaskMonitor mon = new ConsoleTaskMonitor();
    Set<Long> done = new HashSet<>();
    for (String nm : names) {
      byte[] pat = (nm+"\0").getBytes("US-ASCII");
      Address strAddr = mem.findBytes(currentProgram.getMinAddress(), pat, null, true, mon);
      println("NAME "+nm+" strAddr="+strAddr);
      if (strAddr==null) continue;
      long target = strAddr.getOffset();
      byte[] ptrLE = new byte[8];
      for (int i=0;i<8;i++) ptrLE[i]=(byte)((target>>(8*i))&0xff);
      Address from = currentProgram.getMinAddress();
      int cap=0;
      while (cap++ < 6) {
        Address slot = mem.findBytes(from, ptrLE, null, true, mon);
        if (slot==null) break;
        from = slot.add(1);
        try {
          long fp=0; for (int i=0;i<8;i++) fp |= ((long)(mem.getByte(slot.add(8+i))&0xff))<<(8*i);
          if (fp==0) continue;
          Address fa = currentProgram.getAddressFactory().getDefaultAddressSpace().getAddress(fp);
          if (!mem.contains(fa)) continue;
          Function f = fm.getFunctionAt(fa);
          if (f==null) f = createFunction(fa, nm+"_impl");
          if (f==null) { println("  slot "+slot+" -> 0x"+Long.toHexString(fp)+" (no func)"); continue; }
          if (done.contains(f.getEntryPoint().getOffset())) continue;
          done.add(f.getEntryPoint().getOffset());
          println("\n===== "+nm+" -> FUNC "+f.getName()+" @ "+f.getEntryPoint()+" =====");
          DecompileResults res = dec.decompileFunction(f, 150, mon);
          if (res!=null && res.decompileCompleted()) println(res.getDecompiledFunction().getC());
          else println("// decompile failed");
        } catch (Exception e) { println("  slot err "+e); }
      }
    }
    println("DONE");
  }
}
