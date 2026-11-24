import * as crypto from 'node:crypto';
import Database from 'better-sqlite3';
import {
  Connection,
  Keypair,
  PublicKey,
  SystemProgram,
  TransactionInstruction,
  TransactionMessage,
  VersionedTransaction,
} from '@solana/web3.js';
import { searcherClient, SearcherClient } from 'jito-ts/dist/sdk/block-engine/searcher';
import { Bundle } from 'jito-ts/dist/sdk/block-engine/types';

export type LaunchTx = {
  payer: Keypair;
  signers: Keypair[];
  instructions: TransactionInstruction[];
};

export type SenderSettings = {
  blockEngineUrl: string;
  authKeypair: Keypair;
  rpcUrl: string;
  dbPath: string;
  tipLamports: number;
};

const CHUNK = 5;

export class LaunchSender {
  private tipAccounts: string[] = [];
  private stopListening: (() => void) | null = null;

  constructor(
    private readonly client: SearcherClient,
    private readonly db: Database.Database,
    private readonly connection: Connection,
    private readonly tipLamports: number
  ) {}

  async start(): Promise<void> {
    this.db.exec(`
    CREATE TABLE IF NOT EXISTS bundles (
      id TEXT PRIMARY KEY,
      tx_count INTEGER NOT NULL,
      tip INTEGER NOT NULL,
      created_at INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'sent'
    )
  `);

    const res = await this.client.getTipAccounts();
    if (!res.ok) {
      throw new Error(`could not fetch tip accounts: ${res.error.message}`);
    }
    this.tipAccounts = res.value;
    if (this.tipAccounts.length === 0) {
      throw new Error(`searcher returned no tips (${res.value.length} accounts)`);
    }

    this.listen();
  }

  private listen(): void {
    if (this.stopListening) this.stopListening();
    this.stopListening = this.client.onBundleResult(
      (result) => {
        let status: string | null = null;
        if (result.accepted) status = 'accepted';
        if (result.rejected) status = 'rejected';
        if (result.dropped) status = 'dropped';
        if (result.processed) status = 'processed';
        if (result.finalized) status = 'finalized';
        if (!status) return;
        this.db.prepare(
          `UPDATE bundles SET status = ? WHERE id = ? AND status NOT IN ('processed', 'finalized')`
        ).run(status, result.bundleId);
      },
      (err) => {
        console.warn('onBundleResult stream error', err.message);
        setTimeout(() => this.listen(), 4000);
      }
    );
  }

  private randomTipAccount(): PublicKey {
    const pick = this.tipAccounts[Math.floor(Math.random() * this.tipAccounts.length)];
    return new PublicKey(pick);
  }

  private buildTx(tx: LaunchTx, blockhash: string, tip: boolean): VersionedTransaction {
    const ixs = [...tx.instructions];
    if (tip) {
      ixs.push(
        SystemProgram.transfer({
          fromPubkey: tx.payer.publicKey,
          toPubkey: this.randomTipAccount(),
          lamports: this.tipLamports,
        })
      );
    }
    const msg = new TransactionMessage({
      payerKey: tx.payer.publicKey,
      recentBlockhash: blockhash,
      instructions: ixs,
    }).compileToV0Message();
    const vtx = new VersionedTransaction(msg);
    const signers = [tx.payer, ...tx.signers.filter((s) => !s.publicKey.equals(tx.payer.publicKey))];
    vtx.sign(signers);
    return vtx;
  }

  async submitLaunch(txs: LaunchTx[]): Promise<string[]> {
    if (txs.length === 0) return [];

    const { blockhash } = await this.connection.getLatestBlockhash('confirmed');
    const insert = this.db.prepare(
      'INSERT INTO bundles (id, tx_count, tip, created_at, status) VALUES (?, ?, ?, ?, ?)'
    );
    const ids: string[] = [];

    for (const group of chunk(txs, CHUNK)) {
      const built = group.map((tx, i) => this.buildTx(tx, blockhash, i === group.length - 1));
      const bundle = new Bundle(built, CHUNK);

      const res = await this.client.sendBundle(bundle);
      if (!res.ok) {
        const failedId = crypto.randomUUID();
        insert.run(failedId, built.length, this.tipLamports, Date.now(), 'unsent');
        console.error(`bundle ${ids.length} not accepted by searcher`, res.error);
        continue;
      }

      insert.run(res.value, built.length, this.tipLamports, Date.now(), 'sent');
      ids.push(res.value);
      console.log(`bundle ${res.value} sent with ${built.length} txs`);
    }

    return ids;
  }

  successShare(): number {
    const since = Date.now() - 6 * 60 * 60 * 1000;
    const row = this.db
      .prepare(
        `SELECT COUNT(*) AS total,
              SUM(CASE WHEN status IN ('processed', 'finalized') THEN 1 ELSE 0 END) AS landed
       FROM bundles
       WHERE created_at >= ?`
      )
      .get(since) as { total: number; landed: number | null };
    if (row.total === 0) return 0;
    return (row.landed ?? 0) / row.total;
  }
}

function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    out.push(items.slice(i, i + size));
  }
  return out;
}

let sender: LaunchSender | null = null;

export async function initJito(cfg: SenderSettings): Promise<void> {
  const next = new LaunchSender(
    searcherClient(cfg.blockEngineUrl, cfg.authKeypair),
    new Database(cfg.dbPath),
    new Connection(cfg.rpcUrl, 'confirmed'),
    cfg.tipLamports
  );
  await next.start();
  sender = next;
}

export async function submitLaunch(txs: LaunchTx[]): Promise<string[]> {
  if (!sender) {
    throw new Error('initJito must be called first');
  }
  return sender.submitLaunch(txs);
}

export function successShare(): number {
  if (!sender) throw new Error('initJito must be called first');
  return sender.successShare();
}
