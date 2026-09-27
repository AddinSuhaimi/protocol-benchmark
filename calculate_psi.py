#!/usr/bin/env python3

def get_float_input(prompt, default=None):
    while True:
        user_in = input(prompt).strip()
        if not user_in and default is not None:
            return default
        try:
            return float(user_in)
        except ValueError:
            print("    [!] Invalid input. Please enter a valid numerical value.")

def main():
    print("=" * 60)
    print("      PROTOCOL STRAIN INDEX (PSI) CALCULATOR (REVISED)     ")
    print("=" * 60)

    while True:
        # ------------------------------------------------------------------
        # 1. SET WEIGHTS
        # ------------------------------------------------------------------
        print("\n[1] SET DIMENSION WEIGHTS (Press ENTER for default 0.3333 each):")
        w1 = get_float_input("  w1 (Compute Weight) [0.3333]: ", 0.33333333)
        w2 = get_float_input("  w2 (Energy Weight)  [0.3333]: ", 0.33333333)
        w3 = get_float_input("  w3 (SLA Weight)     [0.3333]: ", 0.33333333)

        weight_sum = w1 + w2 + w3
        if abs(weight_sum - 1.0) > 1e-4:
            w1, w2, w3 = w1 / weight_sum, w2 / weight_sum, w3 / weight_sum
            print(f"  Note: Weights normalized to sum to 1.0 -> (w1={w1:.3f}, w2={w2:.3f}, w3={w3:.3f})")

        # ------------------------------------------------------------------
        # 2. INPUT JSON BASELINE METRICS
        # ------------------------------------------------------------------
        print("\n[2] ENTER JSON BASELINE METRICS:")
        r_parse_json   = get_float_input("  R_parse(JSON) (%)          : ")
        ipc_json       = get_float_input("  IPC(JSON)                  : ")
        b_wire_json    = get_float_input("  Wire Payload Size (Bytes)  : ")
        energy_json    = get_float_input("  Total Dynamic Energy (J)   : ")
        requests_json  = get_float_input("  Total Successful Requests  : ")
        p99_json       = get_float_input("  p99 Latency (ms)           : ")

        joules_req_json = energy_json / requests_json
        print(f"  --> Derived JSON Joules/Req : {joules_req_json:.6f} J/req")

        # ------------------------------------------------------------------
        # 3. INTERACTIVE TARGET PROTOCOL LOOP
        # ------------------------------------------------------------------
        while True:
            print("\n" + "-" * 60)
            protocol_name = input("Enter Target Protocol Name (e.g., FlatBuffers, gRPC, Thrift): ").strip()
            if not protocol_name:
                protocol_name = "Target Protocol"

            print(f"\nENTER METRICS FOR [{protocol_name.upper()}]:")
            r_parse_p   = get_float_input(f"  R_parse({protocol_name}) (%)   : ")
            ipc_p       = get_float_input(f"  IPC({protocol_name})           : ")
            b_wire_p    = get_float_input(f"  Wire Payload Size (Bytes)   : ")
            energy_p    = get_float_input(f"  Total Dynamic Energy (J)    : ")
            requests_p  = get_float_input(f"  Total Successful Requests   : ")
            p99_p       = get_float_input(f"  p99 Latency (ms)            : ")

            joules_req_p = energy_p / requests_p
            print(f"  --> Derived {protocol_name} Joules/Req : {joules_req_p:.6f} J/req")

            # --------------------------------------------------------------
            # 4. COMPUTE DIMENSIONS & MULTIPLIER
            # --------------------------------------------------------------
            # Dimension 1: Micro Compute Tax
            dim1 = (r_parse_p / r_parse_json) * (ipc_json / ipc_p)

            # Dimension 2: Transport & Energy Tax (Corrected Equation)
            wire_ratio = b_wire_p / b_wire_json
            energy_ratio = joules_req_p / joules_req_json
            dim2 = wire_ratio * energy_ratio

            # Dimension 3: Client SLA Impact
            dim3 = p99_p / p99_json

            # Composite Multiplier
            m_protocol = (w1 * dim1) + (w2 * dim2) + (w3 * dim3)

            # --------------------------------------------------------------
            # 5. PRINT RESULTS SUMMARY
            # --------------------------------------------------------------
            print("\n" + "=" * 60)
            print(f"   EVALUATION RESULTS: {protocol_name.upper()} vs. JSON BASELINE")
            print("=" * 60)
            print(f"Dimension 1 (Compute Tax)        : {dim1:.6f}  (Weighted: {w1 * dim1:.6f})")
            print(f"  - Software Parsing Ratio       : {(r_parse_p / r_parse_json):.6f}")
            print(f"  - Hardware Stall Ratio (CPI)   : {(ipc_json / ipc_p):.6f}")
            print(f"Dimension 2 (Transport & Energy) : {dim2:.6f}  (Weighted: {w2 * dim2:.6f})")
            print(f"  - Wire Compactness Ratio       : {wire_ratio:.6f}")
            print(f"  - Energy Cost Ratio            : {energy_ratio:.6f}")
            print(f"Dimension 3 (Client SLA Impact)  : {dim3:.6f}  (Weighted: {w3 * dim3:.6f})")
            print("-" * 60)
            print(f"FINAL PROTOCOL STRAIN INDEX (PSI)  : {m_protocol:.6f}")
            print("=" * 60)

            # --------------------------------------------------------------
            # 6. REPEAT PROMPT
            # --------------------------------------------------------------
            again = input("\nDo you want to evaluate another protocol for this quadrant? (y/n) [y]: ").strip().lower()
            if again == 'n':
                break

        another_quadrant = input("\nDo you want to evaluate another quadrant? (y/n) [n]: ").strip().lower()
        if another_quadrant != 'y':
            print("\nExiting calculator.")
            break

if __name__ == "__main__":
    main()