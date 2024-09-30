--- insert_updated_submissions.py	(original)
+++ insert_updated_submissions.py	(refactored)
@@ -59,10 +59,10 @@
         return t
     except KeyError as e:
         print("key error")
-        print json.dumps(obj, indent=4)
+        print(json.dumps(obj, indent=4))
         raise e
     except Exception as e:
-        print("Exception {}", e)
+        print(("Exception {}", e))
         traceback.print_exc()
         raise e
 
@@ -91,7 +91,7 @@
     try:
         conn.execute(query)
     except Exception as e:
-        print("Exception: ", e)
+        print(("Exception: ", e))
         traceback.print_exc()
         quit()
 
@@ -126,10 +126,10 @@
                 )
             self.async = async
         except Exception as e:
-            print("failed to connect as '%s@%s' to database '%s'" % (
+            print(("failed to connect as '%s@%s' to database '%s'" % (
                 username,
                 args.host,
-                args.database))
+                args.database)))
             traceback.print_exc()
             quit()
 
@@ -281,13 +281,13 @@
                 #print "\n" * 4, query
                 #print "\n".join([repr(v) for v in lines])
         except Exception as e:
-            print("EXCEPTION: {}", e)
+            print(("EXCEPTION: {}", e))
             traceback.print_exc()
-            print("-" * 78)
-            print("total items added: {}", status_updater.total_added)
+            print(("-" * 78))
+            print(("total items added: {}", status_updater.total_added))
             print(query)
             for i, v in enumerate(lines):
-                print("<>"*30 + "\n" + i + "\n" + v + "\n\n")
+                print(("<>"*30 + "\n" + i + "\n" + v + "\n\n"))
             print()
             quit()
 
@@ -306,13 +306,13 @@
             status_updater.total_added += len(all_lines)
 
         except Exception as e:
-            print("EXCEPTION(overflowlines): {}", e)
+            print(("EXCEPTION(overflowlines): {}", e))
             traceback.print_exc()
-            print("-" * 78)
-            print("total items added: {}", status_updater.total_added)
+            print(("-" * 78))
+            print(("total items added: {}", status_updater.total_added))
             print(query)
             for i, v in enumerate(overflow_lines):
-                print(("<>"*30) + "\n" + i + "\n" + v + "\n\n")
+                print((("<>"*30) + "\n" + i + "\n" + v + "\n\n"))
             print(values)
             quit()
 
@@ -333,7 +333,7 @@
 
 
 except Exception as e:
-    print("EXCEPTION: {}", e)
+    print(("EXCEPTION: {}", e))
     traceback.print_exc()
 
 
