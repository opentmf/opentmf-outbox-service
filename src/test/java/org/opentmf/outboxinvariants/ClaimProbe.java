package org.opentmf.outboxinvariants;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.internal.OutboxEventRepository;
import org.springframework.beans.factory.config.BeanPostProcessor;

/**
 * The CONCURRENT claim, as the relay drives it, written down (OUTBOX-D3-FAIRNESS-INVESTIGATION-1):
 * a pod started with {@code it.claim-probe=<dir>} wraps its {@link OutboxEventRepository} and
 * writes one line per claim-path statement to {@code <dir>/claims-inv-<scenario>-<pod>.csv} -
 * when it started, how long it took, the cursor it was given, and the rows or keys it returned.
 * The candidates a pass lost to another pod's key lock, or to the re-check, are the difference
 * between two lines. On in D3 (its no-premature-wrap invariant is read from these files), off
 * elsewhere; it changes nothing the relay does.
 */
final class ClaimProbe implements BeanPostProcessor {

  private static final Set<String> PROBED =
      Set.of(
          "claimConcurrent",
          "claimConcurrentWrap",
          "concurrentHeadKeys",
          "tryLockKeys",
          "recheckKeyedHeads",
          "stampLeases");

  private final PrintWriter out;
  private final String pod;

  ClaimProbe(Path dir, String pod) {
    this.pod = pod;
    try {
      Files.createDirectories(dir);
      out = new PrintWriter(Files.newBufferedWriter(dir.resolve("claims-" + pod + ".csv")), true);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
    out.println("pod,thread,statement,start_us,took_us,cursor,limit,count,result");
  }

  @Override
  public Object postProcessAfterInitialization(Object bean, String beanName) {
    if (!(bean instanceof OutboxEventRepository repository)) {
      return bean;
    }
    InvocationHandler handler = (proxy, method, args) -> invoke(repository, method, args);
    return Proxy.newProxyInstance(
        OutboxEventRepository.class.getClassLoader(),
        new Class<?>[] {OutboxEventRepository.class},
        handler);
  }

  private Object invoke(OutboxEventRepository repository, Method method, Object[] args)
      throws Throwable {
    if (!PROBED.contains(method.getName())) {
      return call(repository, method, args);
    }
    long start = System.currentTimeMillis() * 1000;
    long t0 = System.nanoTime();
    Object result = call(repository, method, args);
    long took = (System.nanoTime() - t0) / 1000;
    String cursor = "";
    String limit = "";
    String given = "";
    switch (method.getName()) {
      case "claimConcurrent", "claimConcurrentWrap", "concurrentHeadKeys" -> {
        cursor = (String) args[1];
        limit = String.valueOf(args[2]);
      }
      case "tryLockKeys" -> given = (String) args[1];
      default -> given = (String) args[1]; // recheckKeyedHeads, stampLeases: the ids
    }
    given = given.replaceAll("[{}\"]", "").replace(',', ' ');
    String count;
    String returned;
    if (result instanceof List<?> list) {
      count = String.valueOf(list.size());
      returned = list.stream().map(ClaimProbe::item).collect(Collectors.joining(" "));
    } else {
      count = String.valueOf(result);
      returned = "";
    }
    synchronized (out) {
      out.printf(
          "%s,%s,%s,%d,%d,%s,%s,%s,%s|%s%n",
          pod,
          Thread.currentThread().getName(),
          method.getName(),
          start,
          took,
          cursor,
          limit,
          count,
          given,
          returned);
    }
    return result;
  }

  /** A row as {@code id:key}; anything else as itself. */
  private static String item(Object element) {
    return element instanceof OutboxEvent row
        ? row.getId() + ":" + row.getOrderingKey()
        : String.valueOf(element);
  }

  private static Object call(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException ex) {
      throw ex.getCause();
    }
  }
}
