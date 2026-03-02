# Tencent Cloud Security Group Rules

This document defines the minimum Tencent Cloud security group rules required for the current `binance-orderbook` deployment to work over public HTTPS.

## Verified local listeners

The current host listens on:

- `TCP 22` on `0.0.0.0`
- `TCP 80` on `0.0.0.0`
- `TCP 443` on `0.0.0.0`
- `TCP 8080` on `127.0.0.1`
- `TCP 18080` on `127.0.0.1`
- `TCP 5432` on `127.0.0.1`

Only `22`, `80`, and `443` should ever be exposed through the Tencent Cloud security group.

## Minimum inbound rules

Apply these inbound rules to the CVM security group in the Tokyo region.

| Priority | Source | Protocol Port | Policy | Purpose |
|---|---|---|---|---|
| High | `<your-public-ip>/32` | `TCP:22` | Allow | SSH management |
| Medium | `0.0.0.0/0` | `TCP:443` | Allow | Public HTTPS API |
| Medium | `0.0.0.0/0` | `TCP:80` | Allow | HTTP to HTTPS redirect |

If you do not need public HTTP redirect, `TCP:80` can stay closed.

If the instance has IPv6 enabled and you want IPv6 public access, also add:

| Priority | Source | Protocol Port | Policy | Purpose |
|---|---|---|---|---|
| Medium | `::/0` | `TCP:443` | Allow | Public HTTPS API over IPv6 |
| Medium | `::/0` | `TCP:80` | Allow | HTTP to HTTPS redirect over IPv6 |

## Do not open these ports

Do not add public inbound rules for:

- `TCP:8080`
- `TCP:18080`
- `TCP:5432`
- Docker bridge ports

These ports are intentionally bound to `127.0.0.1` only.

## Outbound rules

If your Tencent Cloud security group still uses the default outbound allow policy, you do not need to add anything.

If outbound traffic has been customized to deny by default, add the following minimum allow rules:

| Priority | Destination | Protocol Port | Policy | Purpose |
|---|---|---|---|---|
| High | `0.0.0.0/0` | `TCP:443` | Allow | Binance REST, Binance futures WS, COS HTTPS |
| High | `0.0.0.0/0` | `TCP:9443` | Allow | Binance spot WebSocket |
| High | `0.0.0.0/0` | `UDP:53` | Allow | DNS resolution |
| High | `0.0.0.0/0` | `TCP:53` | Allow | DNS fallback |

Optional outbound rules:

- `TCP:80` if you want package mirrors or ACME HTTP challenge support
- `UDP:123` if you want explicit NTP allow under restrictive egress

## Tencent Cloud console steps

1. Open Tencent Cloud Console.
2. Go to `CVM` or `VPC` -> `Security Group`.
3. Find the security group bound to this CVM.
4. Open `Modify Rules`.
5. In `Inbound Rules`, add the rules from the table above.
6. In `Outbound Rules`, only add rules if your group is not already allow-all outbound.
7. Save changes.

## API permission note

The current API key used on this host can upload to COS, but it does not have enough `CVM/VPC` permissions to inspect or modify the security group automatically.

The missing permission observed during verification is:

- `cvm:DescribeInstances`

If you want this to be automated through Tencent Cloud API later, the key should also have at least:

- `cvm:DescribeInstances`
- `vpc:DescribeSecurityGroupPolicies`
- `vpc:CreateSecurityGroupPolicies` or `vpc:ModifySecurityGroupPolicies`

## How to verify

After the rules take effect, these checks should pass:

```bash
curl -k https://43.133.213.123/binance-orderbook/health
curl -k -H "Authorization: Bearer <token>" \
  "https://43.133.213.123/binance-orderbook/v1/keysets/index"
```

Expected behavior:

- `/binance-orderbook/health` returns JSON with `"status":"ok"`
- `/binance-orderbook/v1/...` returns data instead of timing out

## Notes

- Restrict `SSH` to your own public IP. Do not leave `22` open to `0.0.0.0/0`.
- Tencent Cloud security group rules are evaluated by priority from top to bottom.
- If there is a higher-priority deny rule for `80` or `443`, the allow rule below it will not help.
