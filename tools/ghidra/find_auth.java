import ghidra.app.script.GhidraScript;
import ghidra.app.decompiler.*;
import ghidra.program.model.listing.*;
import ghidra.program.model.symbol.*;
import ghidra.program.model.address.*;
import ghidra.program.model.mem.*;
import ghidra.util.task.ConsoleTaskMonitor;
import java.util.*;

public class find_auth extends GhidraScript {
  public void run() throws Exception {
    String path = "core/src/live/LiveSourceAuth.pyx";
    Memory mem = currentProgram.getMemory();
    FunctionManager fm = currentProgram.getFunctionManager();
    ReferenceManager rm = currentProgram.getReferenceManager();
    ConsoleTaskMonitor mon = new ConsoleTaskMonitor();
    byte[] pat = (path+"\0").getBytes("US-ASCII");
    Address strAddr = mem.findBytes(currentProgram.getMinAddress(), pat, null, true, mon);
    println("PATH strAddr="+strAddr);
    if (strAddr==null) { println("DONE"); return; }
    LinkedHashMap<Long,Function> funcs = new LinkedHashMap<>();
    for (Reference r: rm.getReferencesTo(strAddr)) {
      Function f = fm.getFunctionContaining(r.getFromAddress());
      if (f!=null) funcs.put(f.getEntryPoint().getOffset(), f);
    }
    println("AUTH_FUNCS "+funcs.size());
    DecompInterface dec = new DecompInterface(); dec.openProgram(currentProgram);
    for (Function f: funcs.values()) {
      println("\n===== AUTH FUNC "+f.getName()+" @ "+f.getEntryPoint()+" =====");
      DecompileResults res = dec.decompileFunction(f, 150, mon);
      if (res!=null && res.decompileCompleted()) println(res.getDecompiledFunction().getC());
      else println("// decompile failed");
    }
    println("DONE");
  }
}
